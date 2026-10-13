import { readManifests, validateConfig, validateIsolation } from './instance-config.mjs';
import { fileURLToPath } from 'node:url';
try {
  const manifests = await readManifests(fileURLToPath(new URL('../instances', import.meta.url)));
  if (process.env.INSTANCE_ID && !process.env.INSTANCE_CONFIG_JSON) throw new Error('Private INSTANCE_CONFIG_JSON is missing for the selected environment');
  if (process.env.INSTANCE_CONFIG_JSON) {
    const config = validateConfig(JSON.parse(process.env.INSTANCE_CONFIG_JSON), process.env.INSTANCE_ID);
    validateIsolation([...manifests.filter(c => c.instance_id !== config.instance_id), config]);
  }
  console.log('Instance configuration and resource isolation checks passed');
} catch (error) {
  // Uncaught JSON.parse diagnostics can print the private JSON source itself.
  console.error(error instanceof SyntaxError ? 'Invalid instance configuration JSON' : error.message);
  process.exitCode = 1;
}
