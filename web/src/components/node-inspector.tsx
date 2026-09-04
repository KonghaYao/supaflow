import { useMemo, useState } from "react";

import type { InputBinding, NodeType, StudioDocument, StudioNode, ValueType } from "../domain/models";
import { variablesInScope } from "../domain/variable-scope";

interface Props {
  document: StudioDocument;
  node?: StudioNode;
  nodeTypes: NodeType[];
  onChange(node: StudioNode): void;
}

interface SchemaField {
  name: string;
  title: string;
  description?: string;
  type: ValueType;
  required: boolean;
  choices?: unknown[];
}

/** Node form generated from the backend JSON Schema and typed port registry. */
export function NodeInspector({ document, node, nodeTypes, onChange }: Props) {
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const variables = useMemo(
    () => (node ? variablesInScope(document, node.id, nodeTypes) : []),
    [document, node, nodeTypes],
  );
  if (!node)
    return (
      <aside className="inspector empty-panel">
        <h2>Inspector</h2>
        <p>Select a node to configure it.</p>
      </aside>
    );
  const nodeType = nodeTypes.find((entry) => entry.type === node.type && entry.version === node.version);
  const fields = configFields(nodeType?.configSchema ?? {});
  const updateConfig = (name: string, value: unknown) => {
    setFieldErrors((current) => withoutKey(current, `config.${name}`));
    onChange({ ...node, config: { ...node.config, [name]: value } });
  };
  const updateInput = (port: string, binding?: InputBinding) => {
    const inputs = { ...node.inputs };
    if (binding) inputs[port] = binding;
    else delete inputs[port];
    onChange({ ...node, inputs });
  };
  return (
    <aside className="inspector" aria-label="Node inspector">
      <div>
        <span className="eyebrow">Schema form</span>
        <h2>{node.label}</h2>
        <code>
          {node.type}@{node.version}
        </code>
      </div>
      <label>
        Label
        <input value={node.label} onChange={(event) => onChange({ ...node, label: event.target.value })} />
      </label>
      <section aria-labelledby="configuration-title">
        <h3 id="configuration-title">Configuration</h3>
        {fields.length === 0 ? (
          <p className="muted">This node has no configuration.</p>
        ) : (
          fields.map((field) => (
            <SchemaControl
              key={field.name}
              field={field}
              value={node.config[field.name]}
              error={fieldErrors[`config.${field.name}`]}
              onChange={(value) => updateConfig(field.name, value)}
              onError={(message) => setFieldErrors((current) => ({ ...current, [`config.${field.name}`]: message }))}
            />
          ))
        )}
      </section>
      <section aria-labelledby="typed-inputs-title">
        <h3 id="typed-inputs-title">Typed inputs</h3>
        {Object.entries(nodeType?.inputs ?? {}).length === 0 ? (
          <p className="muted">No inputs.</p>
        ) : (
          Object.entries(nodeType?.inputs ?? {}).map(([port, spec]) => {
            const binding = node.inputs[port];
            const mode = binding?.source
              ? `${binding.source.nodeId}.${binding.source.port}`
              : binding && Object.hasOwn(binding, "literal")
                ? "__literal__"
                : "";
            return (
              <fieldset className="typed-input" key={port}>
                <legend>
                  {port} <small>{spec.type}</small> {spec.required && <span title="required">*</span>}
                </legend>
                <label>
                  Value source
                  <select
                    value={mode}
                    onChange={(event) => {
                      if (event.target.value === "__literal__") {
                        updateInput(port, { type: spec.type, literal: defaultLiteral(spec.type) });
                        return;
                      }
                      const selected = variables.find((value) => value.selector === event.target.value);
                      updateInput(
                        port,
                        selected
                          ? { type: spec.type, source: { nodeId: selected.nodeId, port: selected.port } }
                          : undefined,
                      );
                    }}
                  >
                    <option value="">Unset</option>
                    <option value="__literal__">Literal value</option>
                    {variables
                      .filter((value) => value.type === spec.type || spec.type === "any")
                      .map((value) => (
                        <option key={value.selector} value={value.selector}>
                          {value.selector} · {value.type}
                        </option>
                      ))}
                  </select>
                </label>
                {mode === "__literal__" && (
                  <LiteralControl
                    type={spec.type}
                    value={binding?.literal}
                    error={fieldErrors[`input.${port}`]}
                    onChange={(literal) => {
                      setFieldErrors((current) => withoutKey(current, `input.${port}`));
                      updateInput(port, { type: spec.type, literal });
                    }}
                    onError={(message) => setFieldErrors((current) => ({ ...current, [`input.${port}`]: message }))}
                  />
                )}
              </fieldset>
            );
          })
        )}
      </section>
      <details>
        <summary>Config schema</summary>
        <pre>{JSON.stringify(nodeType?.configSchema ?? {}, null, 2)}</pre>
      </details>
    </aside>
  );
}

function SchemaControl({
  field,
  value,
  error,
  onChange,
  onError,
}: {
  field: SchemaField;
  value: unknown;
  error?: string;
  onChange(value: unknown): void;
  onError(message: string): void;
}) {
  return (
    <label>
      {field.title} {field.required && <span title="required">*</span>}
      {field.choices ? (
        <select value={serializeChoice(value)} onChange={(event) => onChange(parseChoice(event.target.value))}>
          {!field.required && <option value="">Unset</option>}
          {field.choices.map((choice) => (
            <option key={JSON.stringify(choice)} value={JSON.stringify(choice)}>
              {String(choice)}
            </option>
          ))}
        </select>
      ) : (
        <LiteralControl type={field.type} value={value} error={error} onChange={onChange} onError={onError} />
      )}
      {field.description && <small>{field.description}</small>}
    </label>
  );
}

function LiteralControl({
  type,
  value,
  error,
  onChange,
  onError,
}: {
  type: ValueType;
  value: unknown;
  error?: string;
  onChange(value: unknown): void;
  onError(message: string): void;
}) {
  if (type === "boolean")
    return (
      <select value={String(value ?? false)} onChange={(event) => onChange(event.target.value === "true")}>
        <option value="false">false</option>
        <option value="true">true</option>
      </select>
    );
  if (type === "number" || type === "integer")
    return (
      <input
        type="number"
        step={type === "integer" ? 1 : "any"}
        value={typeof value === "number" ? value : ""}
        onChange={(event) => {
          if (event.target.value === "") return;
          const parsed = Number(event.target.value);
          if (Number.isFinite(parsed) && (type !== "integer" || Number.isInteger(parsed))) onChange(parsed);
        }}
      />
    );
  if (type === "string")
    return <input value={typeof value === "string" ? value : ""} onChange={(event) => onChange(event.target.value)} />;
  return (
    <>
      <textarea
        rows={4}
        key={JSON.stringify(value)}
        defaultValue={formatJSONLiteral(value, type)}
        aria-invalid={Boolean(error)}
        onBlur={(event) => {
          try {
            onChange(parseLiteralValue(event.target.value, type));
          } catch (reason) {
            onError(reason instanceof Error ? reason.message : "Invalid JSON value.");
          }
        }}
      />
      {error && (
        <span className="field-error" role="alert">
          {error}
        </span>
      )}
    </>
  );
}

/** Extracts supported top-level controls from a Draft 2020-12 object schema. */
export function configFields(schema: Record<string, unknown>): SchemaField[] {
  if (!isRecord(schema.properties)) return [];
  const required = new Set(
    Array.isArray(schema.required) ? schema.required.filter((item): item is string => typeof item === "string") : [],
  );
  return Object.entries(schema.properties).flatMap(([name, raw]): SchemaField[] => {
    if (!isRecord(raw)) return [];
    const type = supportedType(raw.type);
    return [
      {
        name,
        title: typeof raw.title === "string" ? raw.title : name,
        description: typeof raw.description === "string" ? raw.description : undefined,
        type,
        required: required.has(name),
        choices: Array.isArray(raw.enum) ? raw.enum : undefined,
      },
    ];
  });
}

/** Parses a literal according to the registry's port/config value type. */
export function parseLiteralValue(raw: string, type: ValueType): unknown {
  if (type === "string") return raw;
  if (type === "number" || type === "integer") {
    const value = Number(raw);
    if (!Number.isFinite(value) || (type === "integer" && !Number.isInteger(value)))
      throw new Error(`Value must be a valid ${type}.`);
    return value;
  }
  if (type === "boolean") {
    if (raw !== "true" && raw !== "false") throw new Error("Value must be true or false.");
    return raw === "true";
  }
  const parsed: unknown = JSON.parse(raw);
  if (type === "object" && !isRecord(parsed)) throw new Error("Value must be a JSON object.");
  if (type === "array" && !Array.isArray(parsed)) throw new Error("Value must be a JSON array.");
  return parsed;
}

function supportedType(value: unknown): ValueType {
  return value === "string" ||
    value === "number" ||
    value === "integer" ||
    value === "boolean" ||
    value === "object" ||
    value === "array"
    ? value
    : "any";
}

function defaultLiteral(type: ValueType): unknown {
  if (type === "string") return "";
  if (type === "number" || type === "integer") return 0;
  if (type === "boolean") return false;
  if (type === "array") return [];
  return {};
}

function formatJSONLiteral(value: unknown, type: ValueType): string {
  if (value !== undefined) return JSON.stringify(value, null, 2);
  return JSON.stringify(defaultLiteral(type), null, 2);
}

function serializeChoice(value: unknown): string {
  return value === undefined ? "" : JSON.stringify(value);
}

function parseChoice(value: string): unknown {
  return value === "" ? undefined : JSON.parse(value);
}

function withoutKey(source: Record<string, string>, key: string): Record<string, string> {
  const next = { ...source };
  delete next[key];
  return next;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
