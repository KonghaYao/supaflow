import { useState } from "react";

import type { StudioClient } from "../api/client";
import { TriggerPanel, WebhookSubscriptionPanel } from "./operator-automation";
import { AuditPanel, DeliveryPanel, OperatorOverview, QuotaPanel } from "./operator-core";
import { CredentialPanel, RetentionPanel } from "./operator-security";

type OperatorTab = "overview" | "automation" | "security" | "audit";

export function OperatorConsole({ client }: { client: StudioClient }) {
  const [tab, setTab] = useState<OperatorTab>("overview");
  return (
    <section className="operator-shell">
      <header className="operator-heading">
        <div>
          <p className="eyebrow">Tenant control room</p>
          <h1>Operator Console</h1>
          <p>Durable delivery, security, capacity, and automation controls.</p>
        </div>
      </header>
      <div className="operator-tabs" role="tablist" aria-label="Operator tools">
        {(["overview", "automation", "security", "audit"] as const).map((item) => (
          <button key={item} role="tab" aria-selected={tab === item} onClick={() => setTab(item)}>
            {item}
          </button>
        ))}
      </div>
      <div role="tabpanel" className="operator-content">
        {tab === "overview" && (
          <>
            <OperatorOverview client={client} />
            <QuotaPanel client={client} />
            <DeliveryPanel client={client} />
          </>
        )}
        {tab === "automation" && (
          <>
            <TriggerPanel client={client} />
            <WebhookSubscriptionPanel client={client} />
          </>
        )}
        {tab === "security" && (
          <>
            <CredentialPanel client={client} />
            <RetentionPanel client={client} />
          </>
        )}
        {tab === "audit" && <AuditPanel client={client} />}
      </div>
    </section>
  );
}
