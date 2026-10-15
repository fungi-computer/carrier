import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath, URL } from "node:url";
import process from "node:process";
import ts from "typescript";
import { expect, it } from "vitest";

for (const [name, expected] of [
  ["watchdog", ["settled", "completed"]],
  ["carrier", ["queued", "provider_accepted"]],
]) {
  it(`typechecks and runs ${name}'s SQLite README quick start`, async () => {
    const directory = fileURLToPath(new URL(`../../${name}/`, import.meta.url));
    const readme = await readFile(join(directory, "README.md"), "utf8");
    const snippet = readme.split("```ts\n")[1]?.split("```")[0];
    if (snippet === undefined) throw new Error("README quick start is missing");
    await mkdir(join(directory, ".botanical"), { recursive: true });
    const temporary = await mkdtemp(join(directory, ".botanical/readme-"));
    try {
      const filename = join(temporary, "quick-start.ts");
      await writeFile(filename, snippet);
      const config = ts.readConfigFile(
        join(directory, "tsconfig.json"),
        ts.sys.readFile,
      );
      const parsed = ts.parseJsonConfigFileContent(
        config.config,
        ts.sys,
        directory,
      );
      const program = ts.createProgram([filename], parsed.options);
      const diagnostics = ts.getPreEmitDiagnostics(program);
      expect(
        diagnostics.map((diagnostic) =>
          ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n"),
        ),
      ).toEqual([]);
      const output = execFileSync(process.execPath, [filename], {
        cwd: directory,
        encoding: "utf8",
      });
      for (const line of expected) expect(output.split("\n")).toContain(line);
    } finally {
      await rm(temporary, { recursive: true, force: true });
    }
  }, 30_000);
}
