#!/usr/bin/env node

import process from "node:process";
import { pathToFileURL } from "node:url";

import { chromium } from "@playwright/test";

const ROUTE_PATHS = [
  "monitor",
  "batches/example-id",
  "providers/example-id",
  "library/batches/example-id",
];
const ASSET_RESOURCE_TYPES = new Set(["script", "stylesheet"]);
const AUTH_RESOURCE_TYPES = new Set(["fetch", "xhr"]);
const BLOCKING_ACTIVITY_QUIET_WINDOW_MS = 500;
const QUIESCENCE_POLL_INTERVAL_MS = 50;

function parseArgs(argv) {
  const options = {
    routes: [],
    timeoutMs: 30_000,
    tracePath: "",
  };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--route") {
      options.routes.push(argv[++index] ?? "");
    } else if (argument === "--timeout-ms") {
      options.timeoutMs = Number(argv[++index]);
    } else if (argument === "--trace") {
      options.tracePath = argv[++index] ?? "";
    } else if (argument === "--help") {
      console.log(
        "Usage: frontend-route-browser-smoke --route https://host/prefix " +
          "[--trace path] [--timeout-ms milliseconds]",
      );
      process.exit(0);
    } else {
      throw new Error("unknown browser smoke argument");
    }
  }
  if (options.routes.length === 0) {
    throw new Error("at least one --route is required");
  }
  if (
    !Number.isInteger(options.timeoutMs) ||
    options.timeoutMs < 1_000 ||
    options.timeoutMs > 120_000
  ) {
    throw new Error("--timeout-ms must be an integer from 1000 to 120000");
  }
  return options;
}

function canonicalRoute(value) {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("route URL is malformed");
  }
  if (
    parsed.protocol !== "https:" ||
    !parsed.hostname ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash
  ) {
    throw new Error("route URL must be a credential-free HTTPS URL");
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, "");
  if (!parsed.pathname) {
    throw new Error("route URL must include a non-root path prefix");
  }
  if (!/^\/(?:dev|prod)$/.test(parsed.pathname)) {
    throw new Error("route URL path must be /dev or /prod");
  }
  return parsed;
}

export function routeTargets(routeValue) {
  const route = canonicalRoute(routeValue);
  const prefix = route.pathname;
  const assetPathPrefix = `${prefix}/assets/`;
  return [
    {
      directUrl: new URL(prefix, route.origin).href,
      expectedDocumentUrl: new URL(`${prefix}/`, route.origin).href,
      path: prefix,
      routePrefix: prefix,
      assetPathPrefix,
    },
    {
      directUrl: new URL(`${prefix}/`, route.origin).href,
      expectedDocumentUrl: new URL(`${prefix}/`, route.origin).href,
      path: `${prefix}/`,
      routePrefix: prefix,
      assetPathPrefix,
    },
    ...ROUTE_PATHS.map((suffix) => {
      const directPath = `${prefix}/${suffix}`;
      return {
        directUrl: new URL(directPath, route.origin).href,
        expectedDocumentUrl: new URL(directPath, route.origin).href,
        path: directPath,
        routePrefix: prefix,
        assetPathPrefix,
      };
    }),
  ];
}

export function validateObservation(observation) {
  const errors = [];
  if (observation.navigationFailed) {
    errors.push("browser navigation failed");
  }
  if (
    observation.initialDocumentUrl !== observation.expectedDocumentUrl ||
    observation.reloadDocumentUrl !== observation.expectedDocumentUrl
  ) {
    errors.push("browser document navigation finished on an unexpected URL");
  }
  if (!observation.initialAuthSettled || !observation.reloadAuthSettled) {
    errors.push("browser authentication state did not settle");
  }
  if (
    [observation.initialAuthState, observation.reloadAuthState].includes("error")
  ) {
    errors.push("browser authentication state reported an error");
  }
  if (
    !["anonymous", "authenticated", "error"].includes(
      observation.initialAuthState,
    ) ||
    !["anonymous", "authenticated", "error"].includes(
      observation.reloadAuthState,
    )
  ) {
    errors.push("browser authentication state marker was invalid");
  }
  if (
    (observation.initialAuthState === "anonymous" &&
      !observation.initialAnonymousAuthValid) ||
    (observation.reloadAuthState === "anonymous" &&
      !observation.reloadAnonymousAuthValid)
  ) {
    errors.push("anonymous authentication evidence was not one exact 401");
  }
  if (
    !settledUrlAllowed({
      actualUrl: observation.initialSettledUrl,
      expectedUrl: observation.expectedDocumentUrl,
      expectedOrigin: observation.expectedOrigin,
      routePrefix: observation.routePrefix,
      authSettled: observation.initialAuthSettled,
      authState: observation.initialAuthState,
      anonymousAuthValid: observation.initialAnonymousAuthValid,
    }) ||
    !settledUrlAllowed({
      actualUrl: observation.reloadSettledUrl,
      expectedUrl: observation.expectedDocumentUrl,
      expectedOrigin: observation.expectedOrigin,
      routePrefix: observation.routePrefix,
      authSettled: observation.reloadAuthSettled,
      authState: observation.reloadAuthState,
      anonymousAuthValid: observation.reloadAnonymousAuthValid,
    })
  ) {
    errors.push("browser settled on an unexpected client URL");
  }
  if (!observation.initialMounted || !observation.reloadMounted) {
    errors.push("React mount marker was absent after navigation or refresh");
  }
  if (observation.minimumRootHtmlLength < 1) {
    errors.push("React root remained empty");
  }
  if (observation.consoleErrorCount > 0) {
    errors.push("browser console error observed");
  }
  if (observation.pageErrorCount > 0) {
    errors.push("uncaught page error observed");
  }
  if (observation.failedSameOriginResourceCount > 0) {
    errors.push("same-origin request failed");
  }
  if (observation.failedCrossOriginScriptCount > 0) {
    errors.push("cross-origin script request failed");
  }
  if (observation.badSameOriginResponseCount > 0) {
    errors.push("same-origin script or stylesheet returned non-2xx");
  }
  if (observation.noncanonicalSameOriginAssetCount > 0) {
    errors.push("same-origin script or stylesheet used a noncanonical asset path");
  }
  if (observation.badCrossOriginScriptResponseCount > 0) {
    errors.push("cross-origin script returned non-2xx");
  }
  return errors;
}

export function settledUrlAllowed({
  actualUrl,
  expectedUrl,
  expectedOrigin,
  routePrefix,
  authSettled,
  authState,
  anonymousAuthValid,
}) {
  const actual = sameOriginUrl(actualUrl, expectedOrigin);
  const expected = sameOriginUrl(expectedUrl, expectedOrigin);
  if (
    !actual ||
    !expected ||
    actual.search ||
    actual.hash ||
    expected.search ||
    expected.hash
  ) {
    return false;
  }
  if (actual.href === expected.href) {
    return true;
  }
  if (!authSettled || authState !== "anonymous" || !anonymousAuthValid) {
    return false;
  }
  return actual.href === new URL(`${routePrefix}/auth/login`, expectedOrigin).href;
}

async function settledRootSnapshot(page, activity, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  await page.waitForSelector(
    '#root[data-loom-mounted="true"][data-loom-auth-settled="true"]',
    {
      state: "attached",
      timeout: timeoutMs,
    },
  );
  activity.noteEvent();
  await activity.waitForQuiet(page, Math.max(1, deadline - Date.now()));
  return page.locator("#root").evaluate((root) => ({
    mounted: root.getAttribute("data-loom-mounted") === "true",
    authSettled: root.getAttribute("data-loom-auth-settled") === "true",
    authState: root.getAttribute("data-loom-auth-state") ?? "",
    htmlLength: root.innerHTML.trim().length,
    settledUrl: window.location.href,
  }));
}

function sameOriginUrl(urlValue, expectedOrigin) {
  try {
    const parsed = new URL(urlValue);
    return !parsed.username && !parsed.password && parsed.origin === expectedOrigin
      ? parsed
      : null;
  } catch {
    return null;
  }
}

function networkUrl(urlValue) {
  try {
    const parsed = new URL(urlValue);
    return ["http:", "https:"].includes(parsed.protocol) &&
      !parsed.username &&
      !parsed.password
      ? parsed
      : null;
  } catch {
    return null;
  }
}

export function classifyFailedRequest({ url, expectedOrigin, resourceType }) {
  const parsed = networkUrl(url);
  if (!parsed) {
    return {
      sameOriginFailure: false,
      crossOriginScriptFailure: false,
      crossOriginNonScriptFailure: false,
    };
  }
  const isSameOrigin = parsed.origin === expectedOrigin;
  return {
    sameOriginFailure: isSameOrigin,
    crossOriginScriptFailure: !isSameOrigin && resourceType === "script",
    crossOriginNonScriptFailure: !isSameOrigin && resourceType !== "script",
  };
}

export function requestBlocksQuiescence({ url, expectedOrigin, resourceType }) {
  if (sameOriginUrl(url, expectedOrigin)) {
    return true;
  }
  const parsed = networkUrl(url);
  return Boolean(
    parsed && parsed.origin !== expectedOrigin && resourceType === "script",
  );
}

export function classifyCrossOriginResponse({
  url,
  expectedOrigin,
  resourceType,
  status,
}) {
  const parsed = networkUrl(url);
  const isCrossOrigin = parsed && parsed.origin !== expectedOrigin;
  return {
    crossOriginNonScriptFailure:
      Boolean(isCrossOrigin) && resourceType !== "script" && status >= 400,
    badCrossOriginScriptResponse:
      Boolean(isCrossOrigin) &&
      resourceType === "script" &&
      (status < 200 || status >= 300),
  };
}

function browserGeneratedFailedLoad(consoleError) {
  return (
    consoleError.text.startsWith("Failed to load resource:") &&
    consoleError.lineNumber === 0 &&
    consoleError.columnNumber === 0 &&
    Boolean(consoleError.locationUrl)
  );
}

export function consoleErrorIsIgnorable({
  consoleError,
  crossOriginNonScriptFailures,
  authResponses,
  authRequestFailures,
  expectedOrigin,
  routePrefix,
  authState,
}) {
  if (!browserGeneratedFailedLoad(consoleError)) {
    return false;
  }
  if (
    crossOriginNonScriptFailures.some(
      (failure) =>
        failure.url === consoleError.locationUrl && failure.resourceType !== "script",
    )
  ) {
    return true;
  }
  if (authState !== "anonymous") {
    return false;
  }
  if (
    !phaseHasExactAnonymous401({
      authResponses,
      authRequestFailures,
      expectedOrigin,
      routePrefix,
    })
  ) {
    return false;
  }
  const expectedAuthUrl = new URL(
    `${routePrefix}/api/v1/auth/me`,
    expectedOrigin,
  ).href;
  if (consoleError.locationUrl !== expectedAuthUrl) {
    return false;
  }
  return consoleError.locationUrl === expectedAuthUrl;
}

export function phaseHasExactAnonymous401({
  authResponses,
  authRequestFailures,
  expectedOrigin,
  routePrefix,
}) {
  if (authResponses.length !== 1 || authRequestFailures.length > 1) {
    return false;
  }
  const expectedAuthUrl = new URL(
    `${routePrefix}/api/v1/auth/me`,
    expectedOrigin,
  ).href;
  const [response] = authResponses;
  if (
    response.requestId !== undefined &&
    response.url === expectedAuthUrl &&
    response.status === 401 &&
    AUTH_RESOURCE_TYPES.has(response.resourceType)
  ) {
    return authRequestFailures.every(
      (failure) =>
        failure.requestId === response.requestId &&
        failure.url === response.url &&
        failure.resourceType === response.resourceType &&
        failure.errorText === "net::ERR_ABORTED",
    );
  }
  return false;
}

function createBlockingActivity(expectedOrigin) {
  const activeRequests = new Set();
  let lastEventAt = Date.now();
  const noteEvent = () => {
    lastEventAt = Date.now();
  };
  return {
    noteEvent,
    requestStarted(request) {
      if (
        requestBlocksQuiescence({
          url: request.url(),
          expectedOrigin,
          resourceType: request.resourceType(),
        })
      ) {
        activeRequests.add(request);
        noteEvent();
      }
    },
    requestEnded(request) {
      if (activeRequests.delete(request)) {
        noteEvent();
      }
    },
    noteUrl(url) {
      if (sameOriginUrl(url, expectedOrigin)) {
        noteEvent();
      }
    },
    async waitForQuiet(page, timeoutMs) {
      const deadline = Date.now() + timeoutMs;
      while (true) {
        const now = Date.now();
        const quietFor = now - lastEventAt;
        if (
          activeRequests.size === 0 &&
          quietFor >= BLOCKING_ACTIVITY_QUIET_WINDOW_MS
        ) {
          return;
        }
        const remaining = deadline - now;
        if (remaining <= 0) {
          throw new Error("blocking browser activity did not become quiet");
        }
        const quietRemaining = Math.max(
          1,
          BLOCKING_ACTIVITY_QUIET_WINDOW_MS - quietFor,
        );
        await page.waitForTimeout(
          Math.min(QUIESCENCE_POLL_INTERVAL_MS, quietRemaining, remaining),
        );
      }
    },
  };
}

export function classifySameOriginResponse({
  url,
  expectedOrigin,
  resourceType,
  status,
  assetPathPrefix,
}) {
  const parsed = sameOriginUrl(url, expectedOrigin);
  if (!parsed || !ASSET_RESOURCE_TYPES.has(resourceType)) {
    return {
      badSameOriginAssetResponse: false,
      noncanonicalSameOriginAsset: false,
    };
  }
  return {
    badSameOriginAssetResponse: status < 200 || status >= 300,
    noncanonicalSameOriginAsset:
      !parsed.pathname.startsWith(assetPathPrefix) ||
      Boolean(parsed.search) ||
      Boolean(parsed.hash),
  };
}

async function observeTarget(context, target, timeoutMs) {
  const page = await context.newPage();
  const expected = new URL(target.expectedDocumentUrl);
  const activity = createBlockingActivity(expected.origin);
  const expectedAuthUrl = new URL(
    `${target.routePrefix}/api/v1/auth/me`,
    expected.origin,
  ).href;
  const crossOriginNonScriptFailures = [];
  const authRequestIds = new WeakMap();
  let nextAuthRequestId = 1;
  const phaseEvidence = {
    initial: {
      consoleErrors: [],
      authResponses: [],
      authRequestFailures: [],
    },
    reload: {
      consoleErrors: [],
      authResponses: [],
      authRequestFailures: [],
    },
  };
  let phase = "initial";
  const counters = {
    pageErrorCount: 0,
    failedSameOriginResourceCount: 0,
    failedCrossOriginScriptCount: 0,
    badSameOriginResponseCount: 0,
    badCrossOriginScriptResponseCount: 0,
    noncanonicalSameOriginAssetCount: 0,
  };
  page.on("console", (message) => {
    if (message.type() === "error") {
      const location = message.location();
      phaseEvidence[phase].consoleErrors.push({
        text: message.text(),
        locationUrl: location.url ?? "",
        lineNumber: location.lineNumber ?? location.line ?? -1,
        columnNumber: location.columnNumber ?? location.column ?? -1,
      });
      activity.noteEvent();
    }
  });
  page.on("pageerror", () => {
    counters.pageErrorCount += 1;
    activity.noteEvent();
  });
  page.on("framenavigated", (frame) => {
    if (frame === page.mainFrame()) {
      activity.noteUrl(frame.url());
    }
  });
  page.on("request", (request) => {
    if (request.url() === expectedAuthUrl) {
      authRequestIds.set(request, nextAuthRequestId);
      nextAuthRequestId += 1;
    }
    activity.requestStarted(request);
  });
  page.on("requestfinished", (request) => {
    activity.requestEnded(request);
  });
  page.on("requestfailed", (request) => {
    activity.requestEnded(request);
    if (request.url() === expectedAuthUrl) {
      const requestId = authRequestIds.get(request);
      phaseEvidence[phase].authRequestFailures.push({
        url: request.url(),
        resourceType: request.resourceType(),
        requestId,
        errorText: request.failure()?.errorText ?? "",
      });
    }
    const classification = classifyFailedRequest({
      url: request.url(),
      expectedOrigin: expected.origin,
      resourceType: request.resourceType(),
    });
    const pairedAnonymousAbort =
      request.url() === expectedAuthUrl &&
      request.failure()?.errorText === "net::ERR_ABORTED" &&
      phaseEvidence[phase].authResponses.some(
        (response) =>
          response.requestId === authRequestIds.get(request) &&
          response.status === 401 &&
          AUTH_RESOURCE_TYPES.has(response.resourceType),
      );
    if (classification.sameOriginFailure && !pairedAnonymousAbort) {
      counters.failedSameOriginResourceCount += 1;
    }
    if (classification.crossOriginScriptFailure) {
      counters.failedCrossOriginScriptCount += 1;
    }
    if (classification.crossOriginNonScriptFailure) {
      crossOriginNonScriptFailures.push({
        url: request.url(),
        resourceType: request.resourceType(),
      });
    }
  });
  page.on("response", (response) => {
    const request = response.request();
    activity.noteUrl(response.url());
    const classification = classifySameOriginResponse({
      url: response.url(),
      expectedOrigin: expected.origin,
      resourceType: request.resourceType(),
      status: response.status(),
      assetPathPrefix: target.assetPathPrefix,
    });
    if (classification.badSameOriginAssetResponse) {
      counters.badSameOriginResponseCount += 1;
    }
    if (classification.noncanonicalSameOriginAsset) {
      counters.noncanonicalSameOriginAssetCount += 1;
    }
    const crossOriginClassification = classifyCrossOriginResponse({
      url: response.url(),
      expectedOrigin: expected.origin,
      resourceType: request.resourceType(),
      status: response.status(),
    });
    if (crossOriginClassification.badCrossOriginScriptResponse) {
      counters.badCrossOriginScriptResponseCount += 1;
    }
    if (crossOriginClassification.crossOriginNonScriptFailure) {
      crossOriginNonScriptFailures.push({
        url: response.url(),
        resourceType: request.resourceType(),
      });
    }
    const parsed = networkUrl(response.url());
    if (parsed && parsed.href === expectedAuthUrl) {
      phaseEvidence[phase].authResponses.push({
        url: parsed.href,
        status: response.status(),
        resourceType: request.resourceType(),
        requestId: authRequestIds.get(request),
      });
    }
  });

  const emptySnapshot = {
    mounted: false,
    authSettled: false,
    authState: "",
    htmlLength: 0,
    settledUrl: "",
  };
  let initial = emptySnapshot;
  let reloaded = emptySnapshot;
  let initialDocumentUrl = "";
  let reloadDocumentUrl = "";
  let navigationFailed = false;
  try {
    const initialResponse = await page.goto(target.directUrl, {
      waitUntil: "domcontentloaded",
      timeout: timeoutMs,
    });
    initialDocumentUrl = initialResponse?.url() ?? "";
    initial = await settledRootSnapshot(page, activity, timeoutMs);
    phase = "reload";
    await page.evaluate((directUrl) => {
      window.history.replaceState(null, "", directUrl);
    }, target.directUrl);
    const reloadResponse = await page.reload({
      waitUntil: "domcontentloaded",
      timeout: timeoutMs,
    });
    reloadDocumentUrl = reloadResponse?.url() ?? "";
    reloaded = await settledRootSnapshot(page, activity, timeoutMs);
  } catch {
    navigationFailed = true;
  }
  // Close before freezing event-derived evidence. Any request that starts in
  // the narrow interval after the quiet window must be aborted and observed,
  // not disappear behind an already-copied counter/report snapshot.
  await page.close();
  const initialAnonymousAuthValid = phaseHasExactAnonymous401({
    authResponses: phaseEvidence.initial.authResponses,
    authRequestFailures: phaseEvidence.initial.authRequestFailures,
    expectedOrigin: expected.origin,
    routePrefix: target.routePrefix,
  });
  const reloadAnonymousAuthValid = phaseHasExactAnonymous401({
    authResponses: phaseEvidence.reload.authResponses,
    authRequestFailures: phaseEvidence.reload.authRequestFailures,
    expectedOrigin: expected.origin,
    routePrefix: target.routePrefix,
  });
  const consoleErrorCount = [
    ...phaseEvidence.initial.consoleErrors.map((consoleError) => ({
      consoleError,
      evidence: phaseEvidence.initial,
      authState: initial.authState,
    })),
    ...phaseEvidence.reload.consoleErrors.map((consoleError) => ({
      consoleError,
      evidence: phaseEvidence.reload,
      authState: reloaded.authState,
    })),
  ].filter(
    ({ consoleError, evidence, authState }) =>
      !consoleErrorIsIgnorable({
        consoleError,
        crossOriginNonScriptFailures,
        authResponses: evidence.authResponses,
        authRequestFailures: evidence.authRequestFailures,
        expectedOrigin: expected.origin,
        routePrefix: target.routePrefix,
        authState,
      }),
  ).length;
  const observation = {
    ...counters,
    consoleErrorCount,
    path: target.path,
    expectedDocumentUrl: target.expectedDocumentUrl,
    expectedOrigin: expected.origin,
    routePrefix: target.routePrefix,
    initialDocumentUrl,
    reloadDocumentUrl,
    initialSettledUrl: initial.settledUrl,
    reloadSettledUrl: reloaded.settledUrl,
    initialAuthSettled: initial.authSettled,
    reloadAuthSettled: reloaded.authSettled,
    initialAuthState: initial.authState,
    reloadAuthState: reloaded.authState,
    initialAnonymousAuthValid,
    reloadAnonymousAuthValid,
    initialMounted: initial.mounted,
    reloadMounted: reloaded.mounted,
    minimumRootHtmlLength: Math.min(initial.htmlLength, reloaded.htmlLength),
    navigationFailed,
  };
  return observation;
}

async function run(options) {
  const browser = await chromium.launch({ headless: true });
  let context;
  const observations = [];
  try {
    context = await browser.newContext();
    if (options.tracePath) {
      await context.tracing.start({
        screenshots: true,
        snapshots: true,
        sources: false,
      });
    }
    for (const route of options.routes) {
      for (const target of routeTargets(route)) {
        observations.push(await observeTarget(context, target, options.timeoutMs));
      }
    }
  } finally {
    try {
      if (context && options.tracePath) {
        await context.tracing.stop({ path: options.tracePath });
      }
    } finally {
      if (context) {
        await context.close();
      }
      await browser.close();
    }
  }
  const report = buildReport(observations);
  console.log(JSON.stringify(report, null, 2));
  return report.status === "pass" ? 0 : 1;
}

export function buildReport(observations) {
  const routes = observations.map((observation) => {
    const errors = validateObservation(observation);
    return {
      path: observation.path,
      status: errors.length === 0 ? "pass" : "fail",
      errors,
    };
  });
  return {
    status: routes.every((route) => route.status === "pass") ? "pass" : "fail",
    routes,
  };
}

async function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    process.exitCode = await run(options);
  } catch (error) {
    const name = error instanceof Error ? error.name : "Error";
    console.error(`${name}: frontend browser smoke execution failed`);
    process.exitCode = 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
