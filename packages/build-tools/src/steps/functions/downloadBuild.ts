import { SystemError, UserError } from '@expo/eas-build-job';
import { bunyan } from '@expo/logger';
import { asyncResult } from '@expo/results';
import {
  BuildFunction,
  BuildStepInput,
  BuildStepInputValueTypeName,
  BuildStepOutput,
} from '@expo/steps';
import { Client, CombinedError } from '@urql/core';
import contentDisposition from 'content-disposition';
import { glob } from 'fast-glob';
import { graphql } from 'gql.tada';
import fetch from 'node-fetch';
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import stream from 'stream';
import { promisify } from 'util';
import { z } from 'zod';
import bplistParser from 'bplist-parser';
import plist from 'plist';

import { CustomBuildContext } from '../../customBuildContext';
import { Datadog } from '../../datadog';
import { formatBytes } from '../../utils/artifacts';
import { decompressTarAsync, isFileTarGzAsync } from '../../utils/files';
import { graphqlAbortContext } from '../../utils/graphqlAbort';
import { isConnectionInterruptedError } from '../../utils/networkErrors';
import { promiseRetryWithCondition } from '../../utils/promiseRetryWithCondition';
import { pluralize } from '../../utils/strings';
import {
  type DeviceRunSessionWorkerApi,
  DeviceRunSessionWorkerApiError,
  getDeviceRunSessionWorkerApi,
} from '../utils/deviceRunSessionWorkerApi';

const streamPipeline = promisify(stream.pipeline);

const NETWORK_RETRY_OPTIONS = { retries: 5, minTimeout: 1_000, factor: 2 };

/** Errno code of the error or its causes; undici keeps it on cause, urql on networkError. */
function findErrorCode(error: unknown): string | undefined {
  let current: unknown = error;
  while (current instanceof Error) {
    if ('code' in current && typeof current.code === 'string') {
      return current.code;
    }
    current = current instanceof CombinedError ? current.networkError : current.cause;
  }
  return undefined;
}

function describeError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  const code = findErrorCode(error);
  return code && !message.includes(code) ? `${message.trimEnd()} (${code})` : message;
}

// node-fetch throws type 'system' for a socket failure before the headers; after them it forwards
// ECONNRESET or ERR_STREAM_PREMATURE_CLOSE to the body. HTTP error responses never throw.
function isArchiveTransferError(error: unknown): boolean {
  return (
    (error instanceof Error &&
      error.name === 'FetchError' &&
      'type' in error &&
      error.type === 'system') ||
    isConnectionInterruptedError(error) ||
    findErrorCode(error) === 'ERR_STREAM_PREMATURE_CLOSE'
  );
}

const RETRIED_REQUEST_LABELS = {
  build_lookup: 'Build lookup',
  archive_download: 'Archive download',
} as const;

async function retryOnNetworkFailureAsync<T>(
  attemptAsync: () => Promise<T>,
  {
    request,
    signal,
    isRetryable,
  }: {
    request: keyof typeof RETRIED_REQUEST_LABELS;
    signal?: AbortSignal;
    isRetryable: (error: unknown) => boolean;
  }
): Promise<T> {
  let retries = 0;
  const report = (outcome: 'recovered' | 'exhausted', lastError: unknown): void => {
    Datadog.log(`${RETRIED_REQUEST_LABELS[request]} ${outcome} on attempt ${retries + 1}`, {
      event: 'download_build_network_retry',
      request,
      outcome,
      attempts: String(retries + 1),
      code: findErrorCode(lastError) ?? 'unknown',
    });
  };

  let lastRetriedError: unknown;
  try {
    const result = await promiseRetryWithCondition(
      async () => {
        signal?.throwIfAborted();
        return await attemptAsync();
      },
      error => !signal?.aborted && isRetryable(error),
      NETWORK_RETRY_OPTIONS,
      ({ attemptNumber, error }) => {
        retries = attemptNumber - 1;
        lastRetriedError = error;
      }
    )();
    if (retries > 0) {
      report('recovered', lastRetriedError);
    }
    return result;
  } catch (error) {
    const lastAttemptFailedTransiently =
      retries === NETWORK_RETRY_OPTIONS.retries && !signal?.aborted && isRetryable(error);
    if (lastAttemptFailedTransiently) {
      report('exhausted', error);
    }
    throw error;
  }
}

function isServerSideLookupError(error: CombinedError): boolean {
  return Boolean(error.networkError) || error.response?.status >= 500;
}

type DownloadBuildSource =
  | { buildId: string; applicationArchiveUrl?: never }
  | { buildId?: never; applicationArchiveUrl: string };

const BUILD_BY_ID_QUERY = graphql(`
  query DownloadBuildByIdQuery($buildId: ID!) {
    builds {
      byId(buildId: $buildId) {
        id
        platform
        artifacts {
          applicationArchiveUrl
        }
      }
    }
  }
`);

export function createDownloadBuildFunction(ctx: CustomBuildContext): BuildFunction {
  return new BuildFunction({
    namespace: 'eas',
    id: 'download_build',
    name: 'Download build',
    __metricsId: 'eas/download_build',
    inputProviders: [
      BuildStepInput.createProvider({
        id: 'build_id',
        required: false,
        allowedValueTypeName: BuildStepInputValueTypeName.STRING,
      }),
      BuildStepInput.createProvider({
        id: 'application_archive_url',
        required: false,
        allowedValueTypeName: BuildStepInputValueTypeName.STRING,
      }),
      BuildStepInput.createProvider({
        id: 'extensions',
        required: false,
        allowedValueTypeName: BuildStepInputValueTypeName.JSON,
        defaultValue: ['apk', 'aab', 'ipa', 'app'],
      }),
    ],
    outputProviders: [
      BuildStepOutput.createProvider({
        id: 'artifact_path',
        required: true,
      }),
    ],
    fn: async (stepsCtx, { inputs, outputs, signal }) => {
      const { logger } = stepsCtx;

      const extensions = z.array(z.string()).parse(inputs.extensions.value);
      logger.info(`Expected extensions: [${extensions.join(', ')}]`);
      const buildId = inputs.build_id.value
        ? z.string().uuid().parse(inputs.build_id.value)
        : undefined;
      const applicationArchiveUrl = inputs.application_archive_url.value
        ? parseHttpApplicationArchiveUrl(inputs.application_archive_url.value)
        : undefined;

      let source: DownloadBuildSource;
      if (buildId) {
        if (applicationArchiveUrl) {
          throw new UserError(
            'EAS_DOWNLOAD_BUILD_INVALID_SOURCE',
            'Pass only one of build_id or application_archive_url.'
          );
        }
        source = { buildId };
      } else {
        if (!applicationArchiveUrl) {
          throw new UserError(
            'EAS_DOWNLOAD_BUILD_INVALID_SOURCE',
            'Pass build_id or application_archive_url.'
          );
        }
        source = { applicationArchiveUrl };
      }

      logger.info(
        buildId ? `Downloading build ${buildId}...` : `Downloading application archive...`
      );

      const { artifactPath } = await downloadBuildAsync({
        logger,
        ...source,
        graphqlClient: ctx.graphqlClient,
        deviceRunSessionWorkerApi: buildId ? getDeviceRunSessionWorkerApi(ctx) : null,
        robotAccessToken: stepsCtx.global.staticContext.job.secrets?.robotAccessToken ?? null,
        extensions,
        signal,
      });

      outputs.artifact_path.set(artifactPath);
    },
  });
}

async function fetchApplicationArchiveUrlAsync({
  buildId,
  graphqlClient,
  deviceRunSessionWorkerApi,
  signal,
}: {
  buildId: string;
  graphqlClient: Client;
  deviceRunSessionWorkerApi?: DeviceRunSessionWorkerApi | null;
  signal?: AbortSignal;
}): Promise<string> {
  signal?.throwIfAborted();
  if (deviceRunSessionWorkerApi) {
    return await fetchDeviceRunSessionApplicationArchiveUrlAsync({
      buildId,
      deviceRunSessionWorkerApi,
      signal,
    });
  }
  let result;
  try {
    result = await retryOnNetworkFailureAsync(
      async () => {
        const queryResult = await graphqlClient
          .query(BUILD_BY_ID_QUERY, { buildId }, graphqlAbortContext(signal))
          .toPromise();
        if (queryResult.error && isServerSideLookupError(queryResult.error)) {
          throw queryResult.error;
        }
        return queryResult;
      },
      {
        request: 'build_lookup',
        signal,
        isRetryable: error => error instanceof CombinedError && isServerSideLookupError(error),
      }
    );
  } catch (error) {
    signal?.throwIfAborted();
    throw new SystemError(`Could not fetch build ${buildId}: ${describeError(error)}`, {
      cause: error,
    });
  }
  signal?.throwIfAborted();

  if (result.error) {
    throw new UserError(
      'EAS_DOWNLOAD_BUILD_FETCH_FAILED',
      `Could not fetch build ${buildId}: ${result.error.message}`,
      { cause: result.error }
    );
  }

  const applicationArchiveUrl = result.data?.builds.byId?.artifacts?.applicationArchiveUrl;
  if (!applicationArchiveUrl) {
    throw new UserError(
      'EAS_DOWNLOAD_BUILD_NO_APPLICATION_ARCHIVE',
      'Build does not have an application archive url'
    );
  }

  return applicationArchiveUrl;
}

/**
 * A device run session token can read only the build of its own session, so the build comes from
 * `GET /v2/device-run-session-worker/application` instead of the builds GraphQL query.
 *
 * Only device run session jobs, whose steps www generates from the session, reach this path, so
 * every failure here is a system error: neither the request nor `build_id` comes from the user.
 */
async function fetchDeviceRunSessionApplicationArchiveUrlAsync({
  buildId,
  deviceRunSessionWorkerApi,
  signal,
}: {
  buildId: string;
  deviceRunSessionWorkerApi: DeviceRunSessionWorkerApi;
  signal?: AbortSignal;
}): Promise<string> {
  let application;
  try {
    application = await retryOnNetworkFailureAsync(
      () => deviceRunSessionWorkerApi.getApplicationAsync({ signal }),
      {
        request: 'build_lookup',
        signal,
        isRetryable: error =>
          error instanceof DeviceRunSessionWorkerApiError &&
          (error.status === undefined || error.status >= 500),
      }
    );
  } catch (err) {
    signal?.throwIfAborted();
    throw new SystemError(
      `Could not fetch build ${buildId} for this device run session: ${describeError(err)}`,
      { cause: err }
    );
  }
  signal?.throwIfAborted();

  if (application.build?.id !== buildId) {
    throw new SystemError(
      `Could not fetch build ${buildId}: this device run session runs ${
        application.build ? `build ${application.build.id}` : 'no build'
      }, and its session token can read only that build. www generates this step from the ` +
        'session, so the two should always match. Start a new session, and contact support if ' +
        'it happens again.'
    );
  }
  if (!application.build.applicationArchiveUrl) {
    throw new UserError(
      'EAS_DOWNLOAD_BUILD_NO_APPLICATION_ARCHIVE',
      'Build does not have an application archive url'
    );
  }
  return application.build.applicationArchiveUrl;
}

export async function downloadBuildAsync(
  params: DownloadBuildSource & {
    logger: bunyan;
    graphqlClient: Client;
    /**
     * Set in a device run session job that has a session token. The build lookup then uses the
     * device run session worker API instead of GraphQL.
     */
    deviceRunSessionWorkerApi?: DeviceRunSessionWorkerApi | null;
    robotAccessToken: string | null;
    extensions: string[];
    /** Stops the build lookup, the request and the file write when aborted, and skips the rest. */
    signal?: AbortSignal;
  }
): Promise<{ artifactPath: string }> {
  const { logger, graphqlClient, deviceRunSessionWorkerApi, robotAccessToken, extensions, signal } =
    params;

  let downloadUrl: string;
  let headers: { Authorization: string } | undefined;
  if (params.applicationArchiveUrl) {
    if (params.buildId) {
      throw new UserError(
        'EAS_DOWNLOAD_BUILD_INVALID_SOURCE',
        'Pass only one of buildId or applicationArchiveUrl.'
      );
    }
    downloadUrl = parseHttpApplicationArchiveUrl(params.applicationArchiveUrl);
    headers = undefined;
  } else if (params.buildId) {
    const buildId = z.string().uuid().parse(params.buildId);
    downloadUrl = await fetchApplicationArchiveUrlAsync({
      buildId,
      graphqlClient,
      deviceRunSessionWorkerApi,
      signal,
    });
    // The archive URL from the device run session worker API points to an artifact route that
    // needs no auth. After www stops minting EXPO_TOKEN for session jobs, robotAccessToken is the
    // session token, and www rejects it as a bearer credential on every route.
    headers =
      robotAccessToken && !deviceRunSessionWorkerApi
        ? { Authorization: `Bearer ${robotAccessToken}` }
        : undefined;
  } else {
    throw new UserError(
      'EAS_DOWNLOAD_BUILD_INVALID_SOURCE',
      'Pass buildId or applicationArchiveUrl.'
    );
  }

  const downloadDestinationDirectory = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), 'download_build-downloaded-')
  );

  const isUserSuppliedArchive = params.applicationArchiveUrl !== undefined;
  signal?.throwIfAborted();
  let archivePath: string;
  try {
    archivePath = await retryOnNetworkFailureAsync(
      async () => {
        const response = await fetch(downloadUrl, { headers, signal });
        if (!response.ok) {
          const textResult = await asyncResult(response.text());
          throw new Error(
            `Unexpected response from server (${response.status}): ${textResult.value}`
          );
        }
        const archiveFilename = resolveArchiveFilename({ response, extensions });
        const attemptArchivePath = path.join(downloadDestinationDirectory, archiveFilename);
        // On abort, node-fetch emits an error on the body stream, which fails the pipeline.
        await streamPipeline(response.body, fs.createWriteStream(attemptArchivePath));
        return attemptArchivePath;
      },
      { request: 'archive_download', signal, isRetryable: isArchiveTransferError }
    );
  } catch (error) {
    signal?.throwIfAborted();
    if (isUserSuppliedArchive || !isArchiveTransferError(error)) {
      throw error;
    }
    throw new SystemError(`Could not download the application archive: ${describeError(error)}`, {
      cause: error,
    });
  }
  signal?.throwIfAborted();

  const { size } = await fs.promises.stat(archivePath);

  logger.info(`Downloaded ${archivePath} (${formatBytes(size)} bytes).`);

  const isFileATarGzArchive = await isFileTarGzAsync(archivePath);

  if (!isFileATarGzArchive) {
    logger.info(`Artifact is not a .tar.gz archive, skipping decompression and validation.`);
    return { artifactPath: archivePath };
  }

  const extractionDirectory = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), 'download_build-extracted-')
  );
  // Not interrupted on abort: tar cannot stop its in-flight file writes, so an abort during
  // the extraction lets it finish (local, bounded work) and is reported right after it.
  // This way no file is written after this function settles.
  signal?.throwIfAborted();
  await decompressTarAsync({
    archivePath,
    destinationDirectory: extractionDirectory,
  });
  signal?.throwIfAborted();

  const matchingFiles = await glob(`**/*.(${extensions.join('|')})`, {
    absolute: true,
    cwd: extractionDirectory,
    onlyFiles: false,
    onlyDirectories: false,
  });
  let matchingFilesRoot = extractionDirectory;

  if (
    matchingFiles.length === 0 &&
    extensions.includes('app') &&
    (await isIosAppBundleAsync(extractionDirectory))
  ) {
    const appBundlePath = `${extractionDirectory}.app`;
    await fs.promises.rename(extractionDirectory, appBundlePath);
    matchingFiles.push(appBundlePath);
    matchingFilesRoot = path.dirname(appBundlePath);
  }

  if (matchingFiles.length === 0) {
    throw new UserError(
      'EAS_DOWNLOAD_BUILD_NO_MATCHING_FILES',
      `No ${extensions.map(ext => `.${ext}`).join(', ')} entries found in the archive.`
    );
  }

  logger.info(
    `Found ${matchingFiles.length} matching ${pluralize(
      matchingFiles.length,
      'entry'
    )}:\n${matchingFiles.map(f => `- ${path.relative(matchingFilesRoot, f)}`).join('\n')}`
  );

  return { artifactPath: matchingFiles[0] };
}

async function isIosAppBundleAsync(directory: string): Promise<boolean> {
  try {
    const infoPlist = await fs.promises.readFile(path.join(directory, 'Info.plist'));
    const isBinaryPlist = infoPlist.subarray(0, 8).toString('ascii') === 'bplist00';
    const parsedInfoPlist = (
      isBinaryPlist
        ? bplistParser.parseBuffer(infoPlist)[0]
        : plist.parse(infoPlist.toString('utf8'))
    ) as Record<string, unknown> | undefined;

    return parsedInfoPlist?.CFBundlePackageType === 'APPL';
  } catch {
    return false;
  }
}

function parseHttpApplicationArchiveUrl(value: unknown): string {
  try {
    const applicationArchiveUrl = z.string().parse(value);
    const parsedUrl = new URL(applicationArchiveUrl);
    assert(parsedUrl.protocol === 'http:' || parsedUrl.protocol === 'https:');
    return applicationArchiveUrl;
  } catch {
    throw new UserError(
      'EAS_DOWNLOAD_BUILD_INVALID_APPLICATION_ARCHIVE_URL',
      'application_archive_url must be a valid HTTP or HTTPS URL.'
    );
  }
}

function resolveArchiveFilename({
  response,
  extensions,
}: {
  response: Awaited<ReturnType<typeof fetch>>;
  extensions: string[];
}): string {
  const contentDispositionHeader = response.headers.get('content-disposition');
  let headerFilename: string | undefined;
  if (contentDispositionHeader) {
    try {
      headerFilename = contentDisposition.parse(contentDispositionHeader).parameters.filename;
    } catch {
      // Ignore malformed Content-Disposition headers and fall back to the response URL.
    }
  }

  const urlFilename = path.basename(new URL(response.url).pathname);
  let archiveFilename = path.basename(headerFilename ?? urlFilename ?? '');
  if (!archiveFilename || archiveFilename === '.' || archiveFilename === '..') {
    archiveFilename = 'application';
  }
  if (!path.extname(archiveFilename) && extensions.length === 1) {
    archiveFilename = `${archiveFilename}.${extensions[0]}`;
  }

  // URL and header filenames may contain percent-encoded or unsafe filesystem characters.
  return archiveFilename.replace(/([^a-z0-9.-]+)/gi, '_');
}
