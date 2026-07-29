// @vitest-environment node

import { expect, test } from "vitest";

import {
  buildReport,
  classifyCrossOriginResponse,
  classifyFailedRequest,
  classifySameOriginResponse,
  consoleErrorIsIgnorable,
  phaseHasExactAnonymous401,
  requestBlocksQuiescence,
  routeTargets,
  settledUrlAllowed,
  validateObservation,
} from "./frontend-route-browser-smoke.mjs";

const passingObservation = {
  path: "/dev/monitor",
  expectedDocumentUrl: "https://yylx.world/dev/monitor",
  expectedOrigin: "https://yylx.world",
  routePrefix: "/dev",
  initialDocumentUrl: "https://yylx.world/dev/monitor",
  reloadDocumentUrl: "https://yylx.world/dev/monitor",
  initialSettledUrl: "https://yylx.world/dev/auth/login",
  reloadSettledUrl: "https://yylx.world/dev/auth/login",
  initialAuthSettled: true,
  reloadAuthSettled: true,
  initialAuthState: "anonymous",
  reloadAuthState: "anonymous",
  initialAnonymousAuthValid: true,
  reloadAnonymousAuthValid: true,
  initialMounted: true,
  reloadMounted: true,
  minimumRootHtmlLength: 12,
  navigationFailed: false,
  consoleErrorCount: 0,
  pageErrorCount: 0,
  failedSameOriginResourceCount: 0,
  failedCrossOriginScriptCount: 0,
  badSameOriginResponseCount: 0,
  badCrossOriginScriptResponseCount: 0,
  noncanonicalSameOriginAssetCount: 0,
};

test("route targets cover exact, canonical, and deep direct navigation", () => {
  expect(routeTargets("https://yylx.world/dev")).toEqual([
    {
      directUrl: "https://yylx.world/dev",
      expectedDocumentUrl: "https://yylx.world/dev/",
      path: "/dev",
      routePrefix: "/dev",
      assetPathPrefix: "/dev/assets/",
    },
    {
      directUrl: "https://yylx.world/dev/",
      expectedDocumentUrl: "https://yylx.world/dev/",
      path: "/dev/",
      routePrefix: "/dev",
      assetPathPrefix: "/dev/assets/",
    },
    {
      directUrl: "https://yylx.world/dev/monitor",
      expectedDocumentUrl: "https://yylx.world/dev/monitor",
      path: "/dev/monitor",
      routePrefix: "/dev",
      assetPathPrefix: "/dev/assets/",
    },
    {
      directUrl: "https://yylx.world/dev/batches/example-id",
      expectedDocumentUrl: "https://yylx.world/dev/batches/example-id",
      path: "/dev/batches/example-id",
      routePrefix: "/dev",
      assetPathPrefix: "/dev/assets/",
    },
    {
      directUrl: "https://yylx.world/dev/providers/example-id",
      expectedDocumentUrl: "https://yylx.world/dev/providers/example-id",
      path: "/dev/providers/example-id",
      routePrefix: "/dev",
      assetPathPrefix: "/dev/assets/",
    },
    {
      directUrl: "https://yylx.world/dev/library/batches/example-id",
      expectedDocumentUrl: "https://yylx.world/dev/library/batches/example-id",
      path: "/dev/library/batches/example-id",
      routePrefix: "/dev",
      assetPathPrefix: "/dev/assets/",
    },
  ]);
});

test("passing observation has no errors", () => {
  expect(validateObservation(passingObservation)).toEqual([]);
});

for (const [field, expectedError] of [
  ["navigationFailed", "browser navigation failed"],
  ["consoleErrorCount", "browser console error observed"],
  ["pageErrorCount", "uncaught page error observed"],
  [
    "failedSameOriginResourceCount",
    "same-origin request failed",
  ],
  [
    "failedCrossOriginScriptCount",
    "cross-origin script request failed",
  ],
  [
    "badSameOriginResponseCount",
    "same-origin script or stylesheet returned non-2xx",
  ],
  [
    "noncanonicalSameOriginAssetCount",
    "same-origin script or stylesheet used a noncanonical asset path",
  ],
  ["badCrossOriginScriptResponseCount", "cross-origin script returned non-2xx"],
]) {
  test(`${field} fails the observation`, () => {
    const observation = {
      ...passingObservation,
      [field]: field === "navigationFailed" ? true : 1,
    };
    expect(validateObservation(observation)).toContain(expectedError);
  });
}

test("empty or unmounted roots fail after navigation and refresh", () => {
  const errors = validateObservation({
    ...passingObservation,
    initialMounted: false,
    reloadMounted: false,
    minimumRootHtmlLength: 0,
  });
  expect(errors).toContain("React mount marker was absent after navigation or refresh");
  expect(errors).toContain("React root remained empty");
});

test("an auth-error root marker fails explicitly on either browser phase", () => {
  for (const field of ["initialAuthState", "reloadAuthState"]) {
    expect(
      validateObservation({
        ...passingObservation,
        [field]: "error",
      }),
    ).toContain("browser authentication state reported an error");
  }
});

test("an anonymous root requires exact 401 evidence on either browser phase", () => {
  for (const field of [
    "initialAnonymousAuthValid",
    "reloadAnonymousAuthValid",
  ]) {
    expect(
      validateObservation({
        ...passingObservation,
        [field]: false,
      }),
    ).toContain("anonymous authentication evidence was not one exact 401");
  }
});

test("unexpected document URL fails canonicalization", () => {
  const errors = validateObservation({
    ...passingObservation,
    reloadDocumentUrl: "https://yylx.world/dev",
  });
  expect(errors).toContain(
    "browser document navigation finished on an unexpected URL",
  );
});

test("same-prefix client redirects other than anonymous auth/login fail", () => {
  const errors = validateObservation({
    ...passingObservation,
    reloadSettledUrl: "https://yylx.world/dev/monitor/other",
  });
  expect(errors).toContain("browser settled on an unexpected client URL");
});

test("anonymous auth/login is the only explicit client redirect fallback", () => {
  const base = {
    expectedUrl: "https://yylx.world/dev/monitor",
    expectedOrigin: "https://yylx.world",
    routePrefix: "/dev",
    authSettled: true,
    authState: "anonymous",
    anonymousAuthValid: true,
  };

  expect(
    settledUrlAllowed({
      ...base,
      actualUrl: "https://yylx.world/dev/monitor",
    }),
  ).toBe(true);
  expect(
    settledUrlAllowed({
      ...base,
      actualUrl: "https://yylx.world/dev/auth/login",
    }),
  ).toBe(true);
  for (const actualUrl of [
    "https://yylx.world/dev/other",
    "https://yylx.world/dev/settings",
    "https://yylx.world/dev/auth/login?next=/monitor",
    "https://yylx.world/prod/auth/login",
    "https://other.example/dev/auth/login",
  ]) {
    expect(settledUrlAllowed({ ...base, actualUrl })).toBe(false);
  }
  expect(
    settledUrlAllowed({
      ...base,
      actualUrl: "https://yylx.world/dev/auth/login",
      authState: "authenticated",
    }),
  ).toBe(false);
  expect(
    settledUrlAllowed({
      ...base,
      actualUrl: "https://yylx.world/dev/auth/login",
      authSettled: false,
    }),
  ).toBe(false);
  expect(
    settledUrlAllowed({
      ...base,
      actualUrl: "https://yylx.world/dev/auth/login",
      anonymousAuthValid: false,
    }),
  ).toBe(false);
});

test("expected exact-route document redirect is not an asset response failure", () => {
  expect(
    classifySameOriginResponse({
      url: "https://yylx.world/dev",
      expectedOrigin: "https://yylx.world",
      resourceType: "document",
      status: 308,
      assetPathPrefix: "/dev/assets/",
    }),
  ).toEqual({
    badSameOriginAssetResponse: false,
    noncanonicalSameOriginAsset: false,
  });
});

test("same-origin script and stylesheet responses must be canonical 2xx assets", () => {
  expect(
    classifySameOriginResponse({
      url: "https://yylx.world/dev/assets/index-abc123.js",
      expectedOrigin: "https://yylx.world",
      resourceType: "script",
      status: 200,
      assetPathPrefix: "/dev/assets/",
    }),
  ).toEqual({
    badSameOriginAssetResponse: false,
    noncanonicalSameOriginAsset: false,
  });
  expect(
    classifySameOriginResponse({
      url: "https://yylx.world/dev/assets/index-abc123.css",
      expectedOrigin: "https://yylx.world",
      resourceType: "stylesheet",
      status: 308,
      assetPathPrefix: "/dev/assets/",
    }),
  ).toEqual({
    badSameOriginAssetResponse: true,
    noncanonicalSameOriginAsset: false,
  });
  expect(
    classifySameOriginResponse({
      url: "https://yylx.world/dev/batches/assets/index-abc123.js",
      expectedOrigin: "https://yylx.world",
      resourceType: "script",
      status: 200,
      assetPathPrefix: "/dev/assets/",
    }),
  ).toEqual({
    badSameOriginAssetResponse: false,
    noncanonicalSameOriginAsset: true,
  });
});

test("cross-origin stylesheets do not affect the same-origin asset contract", () => {
  expect(
    classifySameOriginResponse({
      url: "https://fonts.googleapis.com/css2?family=Inter",
      expectedOrigin: "https://yylx.world",
      resourceType: "stylesheet",
      status: 503,
      assetPathPrefix: "/dev/assets/",
    }),
  ).toEqual({
    badSameOriginAssetResponse: false,
    noncanonicalSameOriginAsset: false,
  });
});

test("every same-origin request failure is fatal regardless of resource type", () => {
  for (const resourceType of [
    "document",
    "fetch",
    "font",
    "image",
    "script",
    "stylesheet",
    "xhr",
  ]) {
    expect(
      classifyFailedRequest({
        url: `https://yylx.world/dev/failure/${resourceType}`,
        expectedOrigin: "https://yylx.world",
        resourceType,
      }),
    ).toEqual({
      sameOriginFailure: true,
      crossOriginScriptFailure: false,
      crossOriginNonScriptFailure: false,
    });
  }
});

test("quiescence blocks all same-origin requests and cross-origin scripts only", () => {
  for (const resourceType of ["document", "fetch", "font", "image", "script"]) {
    expect(
      requestBlocksQuiescence({
        url: `https://yylx.world/dev/${resourceType}`,
        expectedOrigin: "https://yylx.world",
        resourceType,
      }),
    ).toBe(true);
  }
  expect(
    requestBlocksQuiescence({
      url: "https://cdn.example/app.js",
      expectedOrigin: "https://yylx.world",
      resourceType: "script",
    }),
  ).toBe(true);
  for (const resourceType of ["fetch", "font", "image", "stylesheet"]) {
    expect(
      requestBlocksQuiescence({
        url: `https://cdn.example/${resourceType}`,
        expectedOrigin: "https://yylx.world",
        resourceType,
      }),
    ).toBe(false);
  }
});

test("cross-origin failed scripts stay fatal while passive resources are attributable", () => {
  expect(
    classifyFailedRequest({
      url: "https://cdn.example/app.js",
      expectedOrigin: "https://yylx.world",
      resourceType: "script",
    }),
  ).toEqual({
    sameOriginFailure: false,
    crossOriginScriptFailure: true,
    crossOriginNonScriptFailure: false,
  });
  expect(
    classifyFailedRequest({
      url: "https://fonts.example/font.woff2",
      expectedOrigin: "https://yylx.world",
      resourceType: "font",
    }),
  ).toEqual({
    sameOriginFailure: false,
    crossOriginScriptFailure: false,
    crossOriginNonScriptFailure: true,
  });
});

test("cross-origin stylesheet 503 is attributable but script 503 is fatal", () => {
  expect(
    classifyCrossOriginResponse({
      url: "https://cdn.example/fonts.css",
      expectedOrigin: "https://yylx.world",
      resourceType: "stylesheet",
      status: 503,
    }),
  ).toEqual({
    crossOriginNonScriptFailure: true,
    badCrossOriginScriptResponse: false,
  });
  expect(
    classifyCrossOriginResponse({
      url: "https://cdn.example/app.js",
      expectedOrigin: "https://yylx.world",
      resourceType: "script",
      status: 503,
    }),
  ).toEqual({
    crossOriginNonScriptFailure: false,
    badCrossOriginScriptResponse: true,
  });
  expect(
    classifyCrossOriginResponse({
      url: "https://cdn.example/fonts.css",
      expectedOrigin: "https://yylx.world",
      resourceType: "stylesheet",
      status: 302,
    }),
  ).toEqual({
    crossOriginNonScriptFailure: false,
    badCrossOriginScriptResponse: false,
  });
});

test("only attributable browser-generated cross-origin passive failures are ignored", () => {
  const base = {
    consoleError: {
      text: "Failed to load resource: net::ERR_CONNECTION_REFUSED",
      locationUrl: "https://fonts.example/font.woff2",
      lineNumber: 0,
      columnNumber: 0,
    },
    crossOriginNonScriptFailures: [
      {
        url: "https://fonts.example/font.woff2",
        resourceType: "font",
      },
    ],
    authResponses: [],
    authRequestFailures: [],
    expectedOrigin: "https://yylx.world",
    routePrefix: "/dev",
    authState: "anonymous",
  };

  expect(consoleErrorIsIgnorable(base)).toBe(true);
  expect(
    consoleErrorIsIgnorable({
      ...base,
      consoleError: { ...base.consoleError, lineNumber: 12 },
    }),
  ).toBe(false);
  expect(
    consoleErrorIsIgnorable({
      ...base,
      crossOriginNonScriptFailures: [],
    }),
  ).toBe(false);
  expect(
    consoleErrorIsIgnorable({
      ...base,
      crossOriginNonScriptFailures: [
        {
          url: "https://fonts.example/font.woff2",
          resourceType: "script",
        },
      ],
    }),
  ).toBe(false);
});

test("anonymous auth 401 console exemption is exact and cannot hide other errors", () => {
  const expectedAuthUrl = "https://yylx.world/dev/api/v1/auth/me";
  const base = {
    consoleError: {
      text: "Failed to load resource: the server responded with a status of 401",
      locationUrl: expectedAuthUrl,
      lineNumber: 0,
      columnNumber: 0,
    },
    crossOriginNonScriptFailures: [],
    authResponses: [
      {
        url: expectedAuthUrl,
        status: 401,
        resourceType: "fetch",
        requestId: 1,
      },
    ],
    authRequestFailures: [],
    expectedOrigin: "https://yylx.world",
    routePrefix: "/dev",
    authState: "anonymous",
  };

  expect(consoleErrorIsIgnorable(base)).toBe(true);
  for (const override of [
    { authState: "authenticated" },
    {
      consoleError: {
        ...base.consoleError,
        locationUrl: `${expectedAuthUrl}?probe=1`,
      },
    },
    {
      consoleError: {
        ...base.consoleError,
        locationUrl: "https://yylx.world/dev/api/v1/other",
      },
    },
    {
      consoleError: {
        ...base.consoleError,
        lineNumber: 1,
      },
    },
    {
      authResponses: [
        {
          url: expectedAuthUrl,
          status: 403,
          resourceType: "fetch",
          requestId: 1,
        },
      ],
    },
    {
      authResponses: [
        {
          url: expectedAuthUrl,
          status: 401,
          resourceType: "document",
          requestId: 1,
        },
      ],
    },
  ]) {
    expect(consoleErrorIsIgnorable({ ...base, ...override })).toBe(false);
  }
});

test("anonymous phase evidence allows only one exact 401 and its paired browser abort", () => {
  const expectedAuthUrl = "https://yylx.world/dev/api/v1/auth/me";
  const base = {
    authResponses: [
      {
        url: expectedAuthUrl,
        status: 401,
        resourceType: "fetch",
        requestId: 1,
      },
    ],
    authRequestFailures: [],
    expectedOrigin: "https://yylx.world",
    routePrefix: "/dev",
  };

  expect(phaseHasExactAnonymous401(base)).toBe(true);
  expect(
    phaseHasExactAnonymous401({
      ...base,
      authRequestFailures: [
        {
          url: expectedAuthUrl,
          resourceType: "fetch",
          requestId: 1,
          errorText: "net::ERR_ABORTED",
        },
      ],
    }),
  ).toBe(true);
  for (const override of [
    { authResponses: [] },
    {
      authResponses: [
        ...base.authResponses,
        {
          url: expectedAuthUrl,
          status: 403,
          resourceType: "fetch",
          requestId: 2,
        },
      ],
    },
    {
      authResponses: [
        ...base.authResponses,
        {
          url: expectedAuthUrl,
          status: 500,
          resourceType: "fetch",
          requestId: 2,
        },
      ],
    },
    {
      authResponses: [
        {
          url: expectedAuthUrl,
          status: 200,
          resourceType: "fetch",
          requestId: 1,
        },
      ],
    },
    {
      authResponses: [
        {
          url: expectedAuthUrl,
          status: 204,
          resourceType: "fetch",
          requestId: 1,
        },
      ],
    },
    {
      authResponses: [
        {
          url: expectedAuthUrl,
          status: 500,
          resourceType: "fetch",
          requestId: 1,
        },
      ],
    },
    {
      authResponses: [
        {
          url: `${expectedAuthUrl}?probe=1`,
          status: 401,
          resourceType: "fetch",
          requestId: 1,
        },
      ],
    },
    {
      authRequestFailures: [
        {
          url: expectedAuthUrl,
          resourceType: "fetch",
          requestId: 2,
          errorText: "net::ERR_FAILED",
        },
      ],
    },
    {
      authRequestFailures: [
        {
          url: expectedAuthUrl,
          resourceType: "fetch",
          requestId: 1,
          errorText: "net::ERR_CONNECTION_RESET",
        },
      ],
    },
    {
      authRequestFailures: [
        {
          url: `${expectedAuthUrl}?probe=1`,
          resourceType: "fetch",
          requestId: 1,
          errorText: "net::ERR_ABORTED",
        },
      ],
    },
    {
      authRequestFailures: [
        {
          url: expectedAuthUrl,
          resourceType: "document",
          requestId: 1,
          errorText: "net::ERR_ABORTED",
        },
      ],
    },
  ]) {
    expect(phaseHasExactAnonymous401({ ...base, ...override })).toBe(false);
  }
});

test("application console errors are never treated as browser network noise", () => {
  expect(
    consoleErrorIsIgnorable({
      consoleError: {
        text: "Application failed",
        locationUrl: "https://yylx.world/dev/assets/index-app123.js",
        lineNumber: 42,
        columnNumber: 7,
      },
      crossOriginNonScriptFailures: [
        {
          url: "https://fonts.example/font.woff2",
          resourceType: "font",
        },
      ],
      authResponses: [],
      authRequestFailures: [],
      expectedOrigin: "https://yylx.world",
      routePrefix: "/dev",
      authState: "anonymous",
    }),
  ).toBe(false);
});

test("JSON report keeps URLs and browser details redacted", () => {
  const report = buildReport([
    {
      ...passingObservation,
      reloadDocumentUrl: "https://user:secret@yylx.world/dev/monitor?token=secret",
    },
  ]);
  const serialized = JSON.stringify(report);

  expect(report.status).toBe("fail");
  expect(serialized).not.toContain("yylx.world");
  expect(serialized).not.toContain("secret");
  expect(serialized).not.toContain("token");
  expect(report.routes).toEqual([
    {
      path: "/dev/monitor",
      status: "fail",
      errors: ["browser document navigation finished on an unexpected URL"],
    },
  ]);
});
