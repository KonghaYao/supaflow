import { createRoot } from "react-dom/client";

import { App } from "./app";
import "./operator.css";
import "./styles.css";
import "./utility-panels.css";

const root = document.getElementById("root");
if (!root) throw new Error("Studio root element is missing.");
// StrictMode is intentionally absent: FlowGram's PlaygroundReactProvider runs
// non-idempotent container setup inside useMemo factories, and React's
// development-only double invocation of those factories loads every DI module
// (including FlowRendererContainerModule) twice into the same container, which
// fails with "Ambiguous match found for serviceIdentifier: FlowRendererRegistry".
createRoot(root).render(<App />);
