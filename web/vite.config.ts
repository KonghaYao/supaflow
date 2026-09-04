import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  base: "/studio/",
  plugins: [react()],
  server: { proxy: { "/v1": "http://127.0.0.1:8080", "/ops": "http://127.0.0.1:8080" } },
  // FlowGram is isolated behind a lazy adapter chunk; the 800 KB threshold
  // guards regressions while acknowledging its current 746 KB minified runtime.
  // Keep the tracked placeholder so a clean source checkout still satisfies
  // Go's embed directive before the production bundle is generated.
  build: { sourcemap: false, target: "es2022", chunkSizeWarningLimit: 800, emptyOutDir: false },
});
