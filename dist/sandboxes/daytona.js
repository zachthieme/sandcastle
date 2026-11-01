import { createRequire } from 'node:module';
import { createIsolatedSandboxProvider } from '../chunk-LS6Q7MDJ.js';
import { MAX_TAIL_CHARS, BoundedTail } from '../chunk-6NQPCXSG.js';
import { stat, readdir } from 'fs/promises';
import { posix, relative, join } from 'path';

createRequire(import.meta.url);
var shellEscape = (s) => `'${s.replaceAll("'", String.raw`'\''`)}'`;
var SESSION_WATCH_INTERVAL_MS = 15e3;
var toDaytonaConfig = (options) => {
  const { apiKey, apiUrl, target } = options ?? {};
  const config = {};
  if (apiKey !== void 0 && apiKey !== "") {
    config.apiKey = apiKey;
  }
  if (apiUrl !== void 0 && apiUrl !== "") {
    config.apiUrl = apiUrl;
  }
  if (target !== void 0 && target !== "") {
    config.target = target;
  }
  return config;
};
var daytona = (options) => createIsolatedSandboxProvider({
  name: "daytona",
  env: options?.env,
  create: async (createOptions) => {
    const maxOutputTailChars = options?.maxOutputTailChars ?? MAX_TAIL_CHARS;
    const { Daytona } = await import('@daytona/sdk');
    const client = new Daytona(toDaytonaConfig(options));
    const createParams = options?.create;
    const envVars = { ...createOptions.env, ...createParams?.envVars };
    const sandbox = createParams !== void 0 && "image" in createParams ? await client.create({ ...createParams, envVars }) : await client.create({ ...createParams, envVars });
    const workDir = await sandbox.getWorkDir() ?? await sandbox.getUserHomeDir() ?? "/home/daytona";
    const worktreePath = posix.join(workDir, "workspace");
    await sandbox.fs.createFolder(worktreePath, "755");
    return {
      worktreePath,
      exec: async (command, opts) => {
        let effectiveCommand = opts?.sudo === true ? `sudo sh -c ${shellEscape(command)}` : command;
        if (opts?.stdin !== void 0) {
          const stdinPath = `/tmp/sandcastle-stdin-${crypto.randomUUID()}`;
          await sandbox.fs.uploadFile(Buffer.from(opts.stdin), stdinPath);
          effectiveCommand = `{ rm -f ${stdinPath}; ${effectiveCommand}
} < ${stdinPath}`;
        }
        if (opts?.onLine) {
          const { onLine } = opts;
          const sessionId = `sandcastle-${crypto.randomUUID()}`;
          const pidPath = `/tmp/${sessionId}.pid`;
          await sandbox.process.createSession(sessionId);
          try {
            const execResponse = await sandbox.process.executeSessionCommand(
              sessionId,
              {
                command: `echo $$ > ${pidPath}; ( cd ${opts?.cwd ?? worktreePath} && ${effectiveCommand}
)`,
                async: true
              }
            );
            const { cmdId } = execResponse;
            const stdoutTail = new BoundedTail(maxOutputTailChars, "\n");
            const stderrTail = new BoundedTail(maxOutputTailChars, "");
            let partial = "";
            const logsDone = sandbox.process.getSessionCommandLogs(
              sessionId,
              cmdId,
              (chunk) => {
                const text = partial + chunk;
                const lines = text.split("\n");
                partial = lines.pop() ?? "";
                for (const line of lines) {
                  stdoutTail.push(line);
                  onLine(line);
                }
              },
              (chunk) => {
                stderrTail.push(chunk);
              }
            );
            logsDone.catch(() => {
            });
            let watchTimer;
            let watching = true;
            const sessionGone = new Promise((resolve, reject) => {
              const check = async () => {
                const alive = await sandbox.process.executeCommand(
                  `test ! -f ${pidPath} || kill -0 "$(cat ${pidPath})"`
                ).then((r) => r.exitCode === 0).catch(() => true);
                const cmd = alive ? void 0 : await sandbox.process.getSessionCommand(sessionId, cmdId).catch(() => void 0);
                if (!watching) {
                  return;
                }
                if (alive) {
                  watchTimer = setTimeout(check, SESSION_WATCH_INTERVAL_MS);
                  return;
                }
                if (typeof cmd?.exitCode === "number") {
                  return resolve();
                }
                reject(
                  new Error(
                    `Daytona session shell exited before the command finished, so its exit code is unknown: ${command}`
                  )
                );
              };
              watchTimer = setTimeout(check, SESSION_WATCH_INTERVAL_MS);
            });
            try {
              await Promise.race([logsDone, sessionGone]);
            } finally {
              watching = false;
              clearTimeout(watchTimer);
            }
            if (partial) {
              stdoutTail.push(partial);
              onLine(partial);
            }
            const cmdInfo = await sandbox.process.getSessionCommand(
              sessionId,
              cmdId
            );
            return {
              stdout: stdoutTail.toString(),
              stderr: stderrTail.toString(),
              exitCode: cmdInfo.exitCode ?? 0
            };
          } finally {
            await sandbox.process.deleteSession(sessionId).catch(() => {
            });
            await sandbox.process.executeCommand(`rm -f ${pidPath}`).catch(() => {
            });
          }
        }
        const response = await sandbox.process.executeCommand(
          effectiveCommand,
          opts?.cwd ?? worktreePath
        );
        return {
          stdout: response.result,
          stderr: response.exitCode === 0 ? "" : response.result,
          exitCode: response.exitCode
        };
      },
      copyIn: async (hostPath, sandboxPath) => {
        const info = await stat(hostPath);
        if (info.isDirectory()) {
          const walk = async (dir) => {
            const entries = await readdir(dir, { withFileTypes: true });
            const files2 = [];
            for (const entry of entries) {
              const full = join(dir, entry.name);
              if (entry.isDirectory()) {
                files2.push(...await walk(full));
              } else {
                files2.push(full);
              }
            }
            return files2;
          };
          const files = await walk(hostPath);
          for (const file of files) {
            const rel = relative(hostPath, file);
            await sandbox.fs.uploadFile(file, join(sandboxPath, rel));
          }
        } else {
          await sandbox.fs.uploadFile(hostPath, sandboxPath);
        }
      },
      copyFileOut: async (sandboxPath, hostPath) => {
        await sandbox.fs.downloadFile(sandboxPath, hostPath);
      },
      close: async () => {
        await client.delete(sandbox);
      }
    };
  }
});

export { daytona };
//# sourceMappingURL=daytona.js.map
//# sourceMappingURL=daytona.js.map