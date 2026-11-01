import { createRequire } from 'node:module';
import { createIsolatedSandboxProvider } from '../chunk-LS6Q7MDJ.js';
import { MAX_TAIL_CHARS, BoundedTail } from '../chunk-6NQPCXSG.js';
import { execSync } from 'child_process';
import { randomUUID } from 'crypto';
import { mkdir, writeFile, stat, readFile, unlink } from 'fs/promises';
import { tmpdir } from 'os';
import { dirname, join } from 'path';
import { Writable } from 'stream';

createRequire(import.meta.url);
var VERCEL_REPO_PATH = "/vercel/sandbox/workspace";
var setOrUndefined = (value) => value === "" ? void 0 : value;
var resolveVercelAuth = (options) => {
  const { env } = process;
  const tokenOption = setOrUndefined(options?.token);
  const teamIdOption = setOrUndefined(options?.teamId);
  const projectIdOption = setOrUndefined(options?.projectId);
  const token = tokenOption ?? setOrUndefined(env.VERCEL_TOKEN);
  if (token === void 0) {
    return {
      ...teamIdOption === void 0 ? {} : { teamId: teamIdOption },
      ...projectIdOption === void 0 ? {} : { projectId: projectIdOption }
    };
  }
  const teamId = teamIdOption ?? setOrUndefined(env.VERCEL_TEAM_ID);
  const projectId = projectIdOption ?? setOrUndefined(env.VERCEL_PROJECT_ID);
  const missing = [
    teamId === void 0 ? "VERCEL_TEAM_ID (or the teamId option)" : null,
    projectId === void 0 ? "VERCEL_PROJECT_ID (or the projectId option)" : null
  ].filter((name) => name !== null);
  if (missing.length > 0) {
    const source = tokenOption === void 0 ? "VERCEL_TOKEN" : "the token option";
    throw new Error(
      `vercel(): ${source} is set, but a Vercel token also needs a team and project id. Missing: ${missing.join(", ")}.`
    );
  }
  return { token, teamId, projectId };
};
var setUpOrStop = async (sandbox, setUp) => {
  try {
    await setUp();
  } catch (error) {
    try {
      await sandbox.stop();
    } catch {
    }
    throw error;
  }
};
var toCreateParams = (options, env) => {
  const createParams = {};
  if (options?.source) {
    createParams.source = options.source;
  }
  if (options?.ports) {
    createParams.ports = options.ports;
  }
  if (options?.resources) {
    createParams.resources = options.resources;
  }
  if (options?.networkPolicy) {
    createParams.networkPolicy = options.networkPolicy;
  }
  const resolvedRuntime = options?.runtime ?? options?.template;
  if (resolvedRuntime !== void 0 && resolvedRuntime !== "") {
    createParams.runtime = resolvedRuntime;
  }
  const timeoutValue = options?.timeout ?? options?.timeoutMs;
  if (timeoutValue !== void 0) {
    createParams.timeout = timeoutValue;
  }
  createParams.env = env;
  Object.assign(createParams, resolveVercelAuth(options));
  return createParams;
};
var vercel = (options) => createIsolatedSandboxProvider({
  name: "vercel",
  env: options?.env,
  create: async (createOptions) => {
    const maxOutputTailChars = options?.maxOutputTailChars ?? MAX_TAIL_CHARS;
    const { Sandbox } = await import('@vercel/sandbox');
    const createParams = toCreateParams(options, createOptions.env);
    const sandbox = await Sandbox.create(createParams);
    await setUpOrStop(sandbox, async () => {
      const mkdirResult = await sandbox.runCommand({
        cmd: "mkdir",
        args: ["-p", VERCEL_REPO_PATH]
      });
      if (mkdirResult.exitCode !== 0) {
        const stderr = await mkdirResult.stderr();
        throw new Error(
          `vercel(): could not create ${VERCEL_REPO_PATH}: ${stderr.trim()}`
        );
      }
    });
    const handle = {
      worktreePath: VERCEL_REPO_PATH,
      exec: async (command, opts) => {
        let effectiveCommand = command;
        if (opts?.stdin !== void 0) {
          const stdinPath = `/tmp/sandcastle-stdin-${randomUUID()}`;
          await sandbox.writeFiles([
            { path: stdinPath, content: Buffer.from(opts.stdin) }
          ]);
          effectiveCommand = `{ rm -f ${stdinPath}; ${command}
} < ${stdinPath}`;
        }
        if (opts?.onLine) {
          const { onLine } = opts;
          const stdoutTail = new BoundedTail(maxOutputTailChars, "\n");
          const stderrTail = new BoundedTail(maxOutputTailChars, "");
          let partial = "";
          const stdoutWritable = new Writable({
            write(chunk, _encoding, callback) {
              const text = partial + chunk.toString();
              const lines = text.split("\n");
              partial = lines.pop() ?? "";
              for (const line of lines) {
                stdoutTail.push(line);
                onLine(line);
              }
              callback();
            },
            final(callback) {
              if (partial) {
                stdoutTail.push(partial);
                onLine(partial);
                partial = "";
              }
              callback();
            }
          });
          const stderrWritable = new Writable({
            write(chunk, _encoding, callback) {
              stderrTail.push(chunk.toString());
              callback();
            }
          });
          const result2 = await sandbox.runCommand({
            cmd: "sh",
            args: ["-c", effectiveCommand],
            cwd: opts?.cwd ?? VERCEL_REPO_PATH,
            stdout: stdoutWritable,
            stderr: stderrWritable,
            ...opts?.sudo === true ? { sudo: true } : {}
          });
          return {
            stdout: stdoutTail.toString(),
            stderr: stderrTail.toString(),
            exitCode: result2.exitCode
          };
        }
        const result = await sandbox.runCommand({
          cmd: "sh",
          args: ["-c", effectiveCommand],
          cwd: opts?.cwd ?? VERCEL_REPO_PATH,
          ...opts?.sudo === true ? { sudo: true } : {}
        });
        const stdout = await result.stdout();
        const stderr = await result.stderr();
        return {
          stdout,
          stderr,
          exitCode: result.exitCode
        };
      },
      copyIn: async (hostPath, sandboxPath) => {
        const info = await stat(hostPath);
        if (info.isDirectory()) {
          const tarPath = join(
            tmpdir(),
            `sandcastle-copyin-${Date.now()}.tar.gz`
          );
          execSync(`tar -czf "${tarPath}" -C "${hostPath}" .`);
          try {
            const tarContent = await readFile(tarPath);
            const sandboxTarPath = `/tmp/sandcastle-copyin-${Date.now()}.tar.gz`;
            await sandbox.writeFiles([
              { path: sandboxTarPath, content: tarContent }
            ]);
            await sandbox.runCommand({
              cmd: "sh",
              args: [
                "-c",
                `mkdir -p "${sandboxPath}" && tar -xzf "${sandboxTarPath}" -C "${sandboxPath}" && rm -f "${sandboxTarPath}"`
              ]
            });
          } finally {
            await unlink(tarPath).catch(() => {
            });
          }
        } else {
          const content = await readFile(hostPath);
          await sandbox.writeFiles([{ path: sandboxPath, content }]);
        }
      },
      copyFileOut: async (sandboxPath, hostPath) => {
        const buffer = await sandbox.readFileToBuffer({
          path: sandboxPath
        });
        if (!buffer) {
          throw new Error(`File not found in Vercel sandbox: ${sandboxPath}`);
        }
        await mkdir(dirname(hostPath), { recursive: true });
        await writeFile(hostPath, buffer);
      },
      close: async () => {
        await sandbox.stop();
      }
    };
    return handle;
  }
});

export { vercel };
//# sourceMappingURL=vercel.js.map
//# sourceMappingURL=vercel.js.map