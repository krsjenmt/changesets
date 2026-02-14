import { spawnSync } from "child_process";
import { ExitError } from "@changesets/errors";
import { error, info, warn } from "@changesets/logger";
import { AccessType, PackageJSON } from "@changesets/types";
import { detect } from "package-manager-detector";
import pc from "picocolors";
import spawn from "spawndamnit";
import semverParse from "semver/functions/parse";
import { createPromiseQueue } from "../../utils/createPromiseQueue";
import { TwoFactorState } from "../../utils/types";
import { getLastJsonObjectFromString } from "../../utils/getLastJsonObjectFromString";

interface PublishOptions {
  cwd: string;
  publishDir: string;
  access: AccessType;
  tag: string;
}

const npmRequestQueue = createPromiseQueue(40);
const npmPublishQueue = createPromiseQueue(10);

function jsonParse(input: string) {
  try {
    return JSON.parse(input);
  } catch (err) {
    if (err instanceof SyntaxError) {
      console.error("error parsing json:", input);
    }
    throw err;
  }
}

interface RegistryInfo {
  scope?: string;
  registry: string;
}

export function getCorrectRegistry(packageJson?: PackageJSON): RegistryInfo {
  const packageName = packageJson?.name;

  if (packageName?.startsWith("@")) {
    const scope = packageName.split("/")[0];
    const scopedRegistry =
      packageJson!.publishConfig?.[`${scope}:registry`] ||
      process.env[`npm_config_${scope}:registry`];
    if (scopedRegistry) {
      return {
        scope,
        registry: scopedRegistry,
      };
    }
  }

  const registry =
    packageJson?.publishConfig?.registry || process.env.npm_config_registry;

  return {
    scope: undefined,
    registry:
      !registry || registry === "https://registry.yarnpkg.com"
        ? "https://registry.npmjs.org"
        : registry,
  };
}

async function getPublishTool(
  cwd: string
): Promise<{ name: "npm" } | { name: "pnpm"; shouldAddNoGitChecks: boolean }> {
  const pm = await detect({ cwd });
  if (!pm || pm.name !== "pnpm") return { name: "npm" };
  try {
    let result = await spawn("pnpm", ["--version"], { cwd });
    let version = result.stdout.toString().trim();
    let parsed = semverParse(version);
    return {
      name: "pnpm",
      shouldAddNoGitChecks:
        parsed?.major === undefined ? false : parsed.major >= 5,
    };
  } catch (e) {
    return {
      name: "pnpm",
      shouldAddNoGitChecks: false,
    };
  }
}

export async function getTokenIsRequired() {
  const { scope, registry } = getCorrectRegistry();
  // Due to a super annoying issue in yarn, we have to manually override this env variable
  // See: https://github.com/yarnpkg/yarn/issues/2935#issuecomment-355292633
  const envOverride = {
    [scope ? `npm_config_${scope}:registry` : "npm_config_registry"]: registry,
  };
  let result = await spawn("npm", ["profile", "get", "--json"], {
    env: Object.assign({}, process.env, envOverride),
  });
  if (result.code !== 0) {
    error(
      "error while checking if token is required",
      result.stderr.toString().trim() || result.stdout.toString().trim()
    );
    return false;
  }
  let json = jsonParse(result.stdout.toString());
  if (json.error || !json.tfa || !json.tfa.mode) {
    return false;
  }
  return json.tfa.mode === "auth-and-writes";
}

export function getPackageInfo(packageJson: PackageJSON) {
  return npmRequestQueue.add(async () => {
    info(`npm info ${packageJson.name}`);

    const { scope, registry } = getCorrectRegistry(packageJson);

    // Due to a couple of issues with yarnpkg, we also want to override the npm registry when doing
    // npm info.
    // Issues: We sometimes get back cached responses, i.e old data about packages which causes
    // `publish` to behave incorrectly. It can also cause issues when publishing private packages
    // as they will always give a 404, which will tell `publish` to always try to publish.
    // See: https://github.com/yarnpkg/yarn/issues/2935#issuecomment-355292633
    let result = await spawn("npm", [
      "info",
      packageJson.name,
      `--${scope ? `${scope}:` : ""}registry=${registry}`,
      "--json",
    ]);

    // Github package registry returns empty string when calling npm info
    // for a non-existent package instead of a E404
    if (result.stdout.toString() === "") {
      return {
        error: {
          code: "E404",
        },
      };
    }
    return jsonParse(result.stdout.toString());
  });
}

export async function infoAllow404(packageJson: PackageJSON) {
  let pkgInfo = await getPackageInfo(packageJson);
  if (pkgInfo.error?.code === "E404") {
    warn(`Received 404 for npm info ${pc.cyan(`"${packageJson.name}"`)}`);
    return { published: false, pkgInfo: {} };
  }
  if (pkgInfo.error) {
    error(
      `Received an unknown error code: ${
        pkgInfo.error.code
      } for npm info ${pc.cyan(`"${packageJson.name}"`)}`
    );
    error(pkgInfo.error.summary);
    if (pkgInfo.error.detail) error(pkgInfo.error.detail);

    throw new ExitError(1);
  }
  return { published: true, pkgInfo };
}

// Track whether delegated auth has succeeded, used for dynamic concurrency adjustment
let delegatedAuthSucceeded = false;

// we have this so that we can do try a publish again after a publish without
// the call being wrapped in the npm request limit and causing the publishes to potentially never run
async function internalPublish(
  packageJson: PackageJSON,
  opts: PublishOptions,
  twoFactorState: TwoFactorState,
  isRetry: boolean = false
): Promise<{ published: boolean }> {
  const publishTool = await getPublishTool(opts.cwd);
  const isDelegated =
    (await twoFactorState.isRequired) &&
    twoFactorState.token === null &&
    process.stdin.isTTY &&
    process.stdout.isTTY;

  // === Common: build base args ===
  let publishFlags = opts.access ? ["--access", opts.access] : [];
  publishFlags.push("--tag", opts.tag);
  if (publishTool.name === "pnpm" && publishTool.shouldAddNoGitChecks) {
    publishFlags.push("--no-git-checks");
  }

  // Mode-specific flags: only add --json and --otp for non-delegated mode
  if (!isDelegated) {
    publishFlags.push("--json");
    if (twoFactorState.token) {
      publishFlags.push("--otp", twoFactorState.token);
    }
  }

  const { scope, registry } = getCorrectRegistry(packageJson);

  // Due to a super annoying issue in yarn, we have to manually override this env variable
  // See: https://github.com/yarnpkg/yarn/issues/2935#issuecomment-355292633
  const envOverride = {
    [scope ? `npm_config_${scope}:registry` : "npm_config_registry"]: registry,
  };

  // === Branch: delegated vs regular ===
  if (isDelegated) {
    const args =
      publishTool.name === "pnpm"
        ? ["publish", ...publishFlags]
        : ["publish", opts.publishDir, ...publishFlags];

    const result = spawnSync(publishTool.name, args, {
      stdio: "inherit",
      env: { ...process.env, ...envOverride },
      cwd: opts.cwd,
    });

    if (result.status === 0) {
      if (!delegatedAuthSucceeded) {
        delegatedAuthSucceeded = true;
        npmPublishQueue.setConcurrency(10); // Bump for remaining packages
      }
      return { published: true };
    }

    // Retry logic for delegated mode:
    // Normally, npm handles OTP retry internally when stdio is inherited - it prompts
    // for OTP, user authenticates, and npm retries the publish. So exit code 0 means
    // success (possibly after npm's internal retry), and non-zero means failure.
    //
    // However, we retry once as a defensive measure for edge cases:
    // - npm's internal prompt was cancelled/interrupted by the user
    // - Timing issues where npm didn't get a chance to retry
    // - Any other transient failure that might succeed on second attempt
    //
    // The tradeoff: for non-OTP failures, user may see error output twice.
    // We accept this since OTP-related issues are the common failure case in
    // interactive publishing, and the retry cost is minimal.
    if (!isRetry) {
      npmPublishQueue.setConcurrency(1);
      delegatedAuthSucceeded = false;
      return internalPublish(packageJson, opts, twoFactorState, true);
    }
    return { published: false };
  }

  // === Regular path: spawndamnit with JSON parsing ===
  let { code, stdout, stderr } =
    publishTool.name === "pnpm"
      ? await spawn("pnpm", ["publish", ...publishFlags], {
          env: Object.assign({}, process.env, envOverride),
          cwd: opts.cwd,
        })
      : await spawn(
          publishTool.name,
          ["publish", opts.publishDir, ...publishFlags],
          {
            env: Object.assign({}, process.env, envOverride),
          }
        );

  if (code !== 0) {
    // NPM's --json output is included alongside the `prepublish` and `postpublish` output in terminal
    // We want to handle this as best we can but it has some struggles:
    // - output of those lifecycle scripts can contain JSON
    // - npm7 has switched to printing `--json` errors to stderr (https://github.com/npm/cli/commit/1dbf0f9bb26ba70f4c6d0a807701d7652c31d7d4)
    // Note that the `--json` output is always printed at the end so this should work
    let json =
      getLastJsonObjectFromString(stderr.toString()) ||
      getLastJsonObjectFromString(stdout.toString());

    if (json?.error) {
      if (process.stdin.isTTY) {
        // OTP error - token expired or invalid, retry will re-prompt
        if (
          json.error.code === "EOTP" ||
          (json.error.code === "E401" &&
            json.error.detail?.includes("--otp=<code>"))
        ) {
          if (twoFactorState.token !== null) {
            twoFactorState.token = null;
          }
          twoFactorState.isRequired = Promise.resolve(true);
          return internalPublish(packageJson, opts, twoFactorState);
        }
      }
      error(
        `an error occurred while publishing ${packageJson.name}: ${json.error.code}`,
        json.error.summary,
        json.error.detail ? "\n" + json.error.detail : ""
      );
    }

    error(stderr.toString() || stdout.toString());
    return { published: false };
  }
  return { published: true };
}

export function publish(
  packageJson: PackageJSON,
  opts: PublishOptions,
  twoFactorState: TwoFactorState
): Promise<{ published: boolean }> {
  // If there are many packages to be published, it's better to limit the
  // concurrency to avoid unwanted errors, for example from npm.
  return npmRequestQueue.add(async () => {
    const isDelegated =
      (await twoFactorState.isRequired) &&
      twoFactorState.token === null &&
      process.stdin.isTTY &&
      process.stdout.isTTY;

    // Start sequential for delegated mode until auth succeeds
    if (isDelegated && !delegatedAuthSucceeded) {
      npmPublishQueue.setConcurrency(1);
    }

    return npmPublishQueue.add(() =>
      internalPublish(packageJson, opts, twoFactorState)
    );
  });
}
