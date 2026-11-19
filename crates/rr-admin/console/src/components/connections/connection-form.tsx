"use client";

import { useEffect, useRef, useState, type FormEvent } from "react";
import { flushSync } from "react-dom";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ConsoleApiError } from "@/lib/api";
import { sanitizeError } from "@/lib/format";
import {
  connectionsApi,
  type ConnectionInput,
  type ConnectionSummary,
  type TlsProfile,
} from "@/lib/connections";

type SecretMode = "keep" | "clear" | "replace";
type PendingAction = "test" | "save" | "connect" | null;

interface SeedFormRow {
  id: number;
  nodeId: string;
  adminAddr: string;
}

interface FormState {
  name: string;
  environment: string;
  seeds: SeedFormRow[];
  discovery: string;
  clusterName: string;
  adminCa: string;
  adminCert: string;
  adminKey: string;
  adminDomain: string;
  discoveryCa: string;
  discoveryCert: string;
  discoveryKey: string;
  discoveryDomain: string;
  discoveryUsername: string;
  discoveryAllowInsecure: boolean;
  prometheusUrl: string;
  prometheusCluster: string;
  pollInterval: string;
  remember: boolean;
  adminToken: string;
  discoveryPassword: string;
  adminTokenMode: SecretMode;
  discoveryPasswordMode: SecretMode;
}

interface ConnectionFormProps {
  connection?: ConnectionSummary;
  keychainAvailable: boolean;
  onSaved?: (connection: ConnectionSummary) => void;
  onComplete?: (connection: ConnectionSummary, connected: boolean) => void;
}

interface OperationError {
  message: string;
  correlationId?: string | null;
}

const fieldIds: Record<string, string> = {
  name: "connection-name",
  environment: "connection-environment",
  discovery: "connection-discovery",
  clusterName: "connection-cluster-name",
  adminCa: "admin-ca",
  adminCert: "admin-cert",
  adminKey: "admin-key",
  adminDomain: "admin-domain",
  discoveryCa: "discovery-ca",
  discoveryCert: "discovery-cert",
  discoveryKey: "discovery-key",
  discoveryDomain: "discovery-domain",
  discoveryUsername: "discovery-username",
  prometheusUrl: "prometheus-url",
  prometheusCluster: "prometheus-cluster",
  pollInterval: "poll-interval",
  adminToken: "new-admin-token",
  discoveryPassword: "new-discovery-password",
};

const advancedFields = new Set(["prometheusUrl", "prometheusCluster", "pollInterval"]);
const secretModes: { value: SecretMode; label: string }[] = [
  { value: "keep", label: "Keep current secret" },
  { value: "clear", label: "Clear secret" },
  { value: "replace", label: "Replace secret" },
];

function initialState(connection?: ConnectionSummary): FormState {
  const seeds = connection?.admin_seeds.length
    ? connection.admin_seeds.map((seed, index) => ({
      id: index,
      nodeId: String(seed.node_id),
      adminAddr: seed.admin_addr,
    }))
    : [{ id: 0, nodeId: "", adminAddr: "" }];
  return {
    name: connection?.name ?? "",
    environment: connection?.environment ?? "",
    seeds,
    discovery: connection?.discovery ?? "",
    clusterName: connection?.cluster_name ?? "",
    adminCa: connection?.admin_tls.ca ?? "",
    adminCert: connection?.admin_tls.cert ?? "",
    adminKey: connection?.admin_tls.key ?? "",
    adminDomain: connection?.admin_tls.domain ?? "",
    discoveryCa: connection?.discovery_tls.ca ?? "",
    discoveryCert: connection?.discovery_tls.cert ?? "",
    discoveryKey: connection?.discovery_tls.key ?? "",
    discoveryDomain: connection?.discovery_tls.domain ?? "",
    discoveryUsername: connection?.discovery_username ?? "",
    discoveryAllowInsecure: connection?.discovery_allow_insecure ?? false,
    prometheusUrl: connection?.prometheus?.url ?? "",
    prometheusCluster: connection?.prometheus?.cluster ?? "",
    pollInterval: String(connection?.poll_interval_secs ?? 5),
    remember: connection?.credential_source === "KEYCHAIN",
    adminToken: "",
    discoveryPassword: "",
    adminTokenMode: "keep",
    discoveryPasswordMode: "keep",
  };
}

function nullable(value: string): string | null {
  return value.trim() || null;
}

function tls(ca: string, cert: string, key: string, domain: string): TlsProfile {
  return { ca: nullable(ca), cert: nullable(cert), key: nullable(key), domain: nullable(domain) };
}

function validHostPort(value: string): boolean {
  if (!value || value.includes("://") || value.includes("@") || /\s/.test(value)) return false;
  const separator = value.lastIndexOf(":");
  if (separator < 1) return false;
  const host = value.slice(0, separator).replace(/^\[|\]$/g, "");
  const rawPort = value.slice(separator + 1);
  const port = Number(rawPort);
  return Boolean(host) && host !== "0.0.0.0" && host !== "::"
    && /^\d+$/.test(rawPort) && Number.isInteger(port) && port >= 1 && port <= 65535;
}

function validDiscoveryEndpoints(value: string): boolean {
  const input = value.startsWith("etcd://") ? value.slice("etcd://".length) : "";
  if (!input) return false;
  const seen = new Set<string>();
  for (const endpoint of input.split(",")) {
    const normalized = endpoint.trim();
    if (!validHostPort(normalized) || seen.has(normalized)) return false;
    seen.add(normalized);
  }
  return true;
}

function validPrometheusUrl(value: string): boolean {
  try {
    const url = new URL(value);
    const authority = value.split("://")[1]?.split(/[/?#]/)[0] ?? "";
    return (url.protocol === "http:" || url.protocol === "https:") && Boolean(url.hostname)
      && !url.username && !url.password && !authority.includes("@") && !url.search && !url.hash;
  } catch {
    return false;
  }
}

function validate(state: FormState, editing: boolean): Record<string, string> {
  const errors: Record<string, string> = {};
  if (!state.name.trim()) errors.name = "Enter a connection name.";

  const seeds = state.seeds.filter((seed) => seed.nodeId.trim() || seed.adminAddr.trim());
  const nodeIds = new Map<number, number>();
  const addresses = new Map<string, number>();
  for (const seed of seeds) {
    const nodeKey = `seed-${seed.id}-node`;
    const addressKey = `seed-${seed.id}-address`;
    const nodeId = Number(seed.nodeId);
    if (!seed.nodeId.trim() || !Number.isSafeInteger(nodeId) || nodeId < 1) {
      errors[nodeKey] = "Enter a positive whole-number node ID.";
    } else if (nodeIds.has(nodeId)) {
      errors[nodeKey] = "Each admin seed must use a unique node ID.";
    } else {
      nodeIds.set(nodeId, seed.id);
    }

    const address = seed.adminAddr.trim();
    if (!validHostPort(address)) {
      errors[addressKey] = "Enter an AdminService address as host:port without userinfo and with a port from 1 to 65535.";
    } else if (addresses.has(address)) {
      errors[addressKey] = "Each admin seed must use a unique AdminService address.";
    } else {
      addresses.set(address, seed.id);
    }
  }
  if (seeds.length === 0 && !state.discovery.trim()) {
    errors.seeds = "Add at least one admin seed or discovery endpoint.";
  }
  if (state.discovery.trim()) {
    if (!validDiscoveryEndpoints(state.discovery.trim())) {
      errors.discovery = "Use comma-separated etcd://host:port discovery endpoints without userinfo and with ports from 1 to 65535.";
    }
    if (!state.clusterName.trim()) {
      errors.clusterName = "Enter the discovery cluster namespace.";
    }
    const hasCa = Boolean(state.discoveryCa.trim());
    if (hasCa === state.discoveryAllowInsecure) {
      errors.discoveryCa = hasCa
        ? "Choose either a discovery CA or insecure transport, not both."
        : "Provide a discovery CA or explicitly allow insecure transport.";
    }
    if (state.discoveryUsername.trim() && !hasCa) {
      errors.discoveryUsername = "Discovery username requires a discovery CA.";
    }
  }
  const pollInterval = Number(state.pollInterval);
  if (!Number.isInteger(pollInterval) || pollInterval < 1 || pollInterval > 300) {
    errors.pollInterval = "Enter a whole number from 1 to 300 seconds.";
  }
  if (Boolean(state.adminCert.trim()) !== Boolean(state.adminKey.trim())) {
    errors.adminKey = "Provide both the admin certificate and private key paths, or neither.";
  }
  if ((state.adminCert.trim() || state.adminDomain.trim()) && !state.adminCa.trim()) {
    errors.adminCa = "Admin certificate and domain settings require an admin CA.";
  }
  if (Boolean(state.discoveryCert.trim()) !== Boolean(state.discoveryKey.trim())) {
    errors.discoveryKey = "Provide both the discovery certificate and private key paths, or neither.";
  }
  if ((state.discoveryCert.trim() || state.discoveryDomain.trim()) && !state.discoveryCa.trim()) {
    errors.discoveryCa = "Discovery certificate and domain settings require a discovery CA.";
  }
  if (Boolean(state.prometheusUrl.trim()) !== Boolean(state.prometheusCluster.trim())) {
    errors.prometheusCluster = "Provide both the Prometheus URL and exact cluster matcher.";
  }
  if (state.prometheusUrl.trim()) {
    if (!validPrometheusUrl(state.prometheusUrl.trim())) {
      errors.prometheusUrl = "Enter an absolute HTTP or HTTPS Prometheus URL.";
    }
  }
  if (editing && state.adminTokenMode === "replace" && !state.adminToken) {
    errors.adminToken = "Enter a replacement admin token, or choose Keep or Clear.";
  }
  if (editing && state.discoveryPasswordMode === "replace" && !state.discoveryPassword) {
    errors.discoveryPassword = "Enter a replacement discovery password, or choose Keep or Clear.";
  }
  return errors;
}

function toInput(state: FormState, editing: boolean): ConnectionInput {
  const input: ConnectionInput = {
    name: state.name.trim(),
    environment: nullable(state.environment),
    admin_seeds: state.seeds
      .filter((seed) => seed.nodeId.trim() || seed.adminAddr.trim())
      .map((seed) => ({ node_id: Number(seed.nodeId), admin_addr: seed.adminAddr.trim() })),
    discovery: nullable(state.discovery),
    cluster_name: nullable(state.clusterName),
    admin_tls: tls(state.adminCa, state.adminCert, state.adminKey, state.adminDomain),
    discovery_tls: tls(state.discoveryCa, state.discoveryCert, state.discoveryKey, state.discoveryDomain),
    discovery_username: nullable(state.discoveryUsername),
    discovery_allow_insecure: state.discoveryAllowInsecure,
    prometheus: state.prometheusUrl.trim()
      ? { url: state.prometheusUrl.trim(), cluster: state.prometheusCluster.trim() }
      : null,
    poll_interval_secs: Number(state.pollInterval),
    remember: state.remember,
  };
  if (!editing) {
    if (state.adminToken) input.admin_token = state.adminToken;
    if (state.discoveryPassword) input.discovery_password = state.discoveryPassword;
  } else {
    if (state.adminTokenMode === "clear") input.admin_token = null;
    if (state.adminTokenMode === "replace") input.admin_token = state.adminToken;
    if (state.discoveryPasswordMode === "clear") input.discovery_password = null;
    if (state.discoveryPasswordMode === "replace") input.discovery_password = state.discoveryPassword;
  }
  return input;
}

function operationFailure(cause: unknown, fallback: string): OperationError {
  const apiError = cause instanceof ConsoleApiError ? cause : null;
  return {
    message: sanitizeError(cause instanceof Error ? cause.message : fallback),
    correlationId: apiError?.correlationId,
  };
}

function FieldError({ id, message }: { id: string; message?: string }) {
  return message ? <p className="field-error" id={id}>{message}</p> : null;
}

function SecretModeField({
  id,
  label,
  value,
  disabled,
  onChange,
}: {
  id: string;
  label: string;
  value: SecretMode;
  disabled: boolean;
  onChange: (value: SecretMode) => void;
}) {
  return (
    <fieldset className="secret-field">
      <legend id={`${id}-label`}>{label}</legend>
      <div className="secret-mode-options" role="radiogroup" aria-labelledby={`${id}-label`}>
        {secretModes.map((mode) => (
          <label className="radio-field" key={mode.value}>
            <input
              type="radio"
              name={id}
              value={mode.value}
              checked={value === mode.value}
              disabled={disabled}
              onChange={() => onChange(mode.value)}
            />
            {mode.label}
          </label>
        ))}
      </div>
    </fieldset>
  );
}

export function ConnectionForm({ connection, keychainAvailable, onSaved, onComplete }: ConnectionFormProps) {
  const [savedConnection, setSavedConnection] = useState<ConnectionSummary | undefined>(connection);
  const [state, setState] = useState(() => initialState(connection));
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [pending, setPending] = useState<PendingAction>(null);
  const [operationError, setOperationError] = useState<OperationError | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const pendingRef = useRef<PendingAction>(null);
  const generationRef = useRef(0);
  const nextSeedId = useRef(state.seeds.length);
  const focusFieldId = useRef<string | null>(null);
  const editing = savedConnection !== undefined;

  useEffect(() => {
    generationRef.current += 1;
    return () => {
      generationRef.current += 1;
    };
  }, [connection?.id]);

  useEffect(() => {
    if (focusFieldId.current === null) return;
    const id = focusFieldId.current;
    focusFieldId.current = null;
    document.getElementById(id)?.focus();
  }, [state.seeds]);

  const update = <K extends keyof FormState>(key: K, value: FormState[K]) => {
    setState((current) => ({ ...current, [key]: value }));
    setErrors((current) => {
      const next = { ...current };
      delete next[key];
      return next;
    });
    setNotice(null);
    setOperationError(null);
  };

  const updateSeed = (id: number, key: "nodeId" | "adminAddr", value: string) => {
    setState((current) => ({
      ...current,
      seeds: current.seeds.map((seed) => seed.id === id ? { ...seed, [key]: value } : seed),
    }));
    setErrors((current) => Object.fromEntries(Object.entries(current).filter(([errorKey]) =>
      errorKey !== "seeds" && !errorKey.startsWith("seed-"),
    )));
    setNotice(null);
    setOperationError(null);
  };

  const validateField = (key: string) => {
    const validation = validate(state, editing);
    setErrors((current) => {
      const next = { ...current };
      delete next[key];
      if (validation[key]) next[key] = validation[key];
      return next;
    });
  };

  const focusFirstError = (nextErrors: Record<string, string>) => {
    const firstKey = Object.keys(nextErrors)[0];
    if (!firstKey) return;
    const targetId = firstKey === "seeds"
      ? state.seeds[0] ? `seed-${state.seeds[0].id}-node` : "add-admin-seed"
      : fieldIds[firstKey] ?? firstKey;
    const focus = () => document.getElementById(targetId)?.focus();
    if (Object.keys(nextErrors).some((key) => advancedFields.has(key))) {
      flushSync(() => setAdvancedOpen(true));
      focus();
    } else {
      focus();
    }
  };

  const run = async (action: Exclude<PendingAction, null>) => {
    if (pendingRef.current) return;
    const nextErrors = validate(state, editing);
    setErrors(nextErrors);
    setNotice(null);
    if (Object.keys(nextErrors).length > 0) {
      focusFirstError(nextErrors);
      return;
    }
    pendingRef.current = action;
    setPending(action);
    setOperationError(null);
    const generation = generationRef.current;
    const isCurrent = () => generationRef.current === generation;
    const input = toInput(state, editing);
    try {
      if (action === "test") {
        await connectionsApi.testCandidate(input, savedConnection?.id);
        if (!isCurrent()) return;
        setNotice("Connection test passed. The candidate was not saved.");
        return;
      }
      const response = savedConnection
        ? await connectionsApi.update(savedConnection.id, input)
        : await connectionsApi.create(input);
      const saved = response.data;
      if (isCurrent()) {
        setSavedConnection(saved);
        setState((current) => ({
          ...current,
          adminToken: "",
          discoveryPassword: "",
          adminTokenMode: "keep",
          discoveryPasswordMode: "keep",
        }));
      }
      onSaved?.(saved);
      if (!isCurrent()) return;
      if (action === "save") {
        onComplete?.(saved, false);
        return;
      }
      try {
        const connected = await connectionsApi.connect(saved.id);
        if (!isCurrent()) return;
        onComplete?.(connected.data, true);
      } catch (error) {
        if (!isCurrent()) return;
        const failure = operationFailure(error, "Try Connect from the connection list.");
        setOperationError({
          ...failure,
          message: `Connection saved, but connecting failed. ${failure.message}`,
        });
      }
    } catch (error) {
      if (isCurrent()) {
        setOperationError(operationFailure(error, "The connection request failed. Check the configuration and try again."));
      }
    } finally {
      if (isCurrent()) {
        pendingRef.current = null;
        setPending(null);
      }
    }
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    void run("save");
  };

  const textField = (
    key: keyof FormState,
    id: string,
    label: string,
    options: { placeholder?: string; type?: string; autoComplete?: string } = {},
  ) => {
    const value = state[key];
    if (typeof value !== "string") return null;
    const errorId = `${id}-error`;
    return (
      <div className="form-field">
        <label htmlFor={id}>{label}</label>
        <Input
          id={id}
          type={options.type ?? "text"}
          value={value}
          placeholder={options.placeholder}
          autoComplete={options.autoComplete}
          disabled={pending !== null}
          aria-invalid={Boolean(errors[key])}
          aria-describedby={errors[key] ? errorId : undefined}
          onChange={(event) => update(key, event.target.value)}
          onBlur={() => validateField(key)}
        />
        <FieldError id={errorId} message={errors[key]} />
      </div>
    );
  };

  return (
    <form className="connection-form" onSubmit={submit} noValidate>
      <fieldset className="connection-section">
        <legend>Identity</legend>
        <p className="section-description">A local profile name and an optional environment label.</p>
        <div className="form-grid form-grid-two">
          {textField("name", "connection-name", "Connection name", { placeholder: "Production", autoComplete: "off" })}
          {textField("environment", "connection-environment", "Environment", { placeholder: "prod" })}
        </div>
      </fieldset>

      <fieldset className="connection-section">
        <legend>Cluster sources</legend>
        <p className="section-description" id="connection-seeds-hint">Use admin seeds, etcd discovery, or both. Each seed needs a unique node ID and AdminService host:port.</p>
        <div className="connection-seed-list">
          {state.seeds.map((seed, index) => {
            const nodeId = `seed-${seed.id}-node`;
            const addressId = `seed-${seed.id}-address`;
            return (
              <div className="connection-seed-row" key={seed.id}>
                <div className="form-field">
                  <label htmlFor={nodeId}>{`Seed ${index + 1} node ID`}</label>
                  <Input
                    id={nodeId}
                    inputMode="numeric"
                    value={seed.nodeId}
                    disabled={pending !== null}
                    aria-invalid={Boolean(errors[nodeId] || errors.seeds)}
                    aria-describedby={["connection-seeds-hint", errors.seeds ? "connection-seeds-error" : null, errors[nodeId] ? `${nodeId}-error` : null].filter(Boolean).join(" ")}
                    onChange={(event) => updateSeed(seed.id, "nodeId", event.target.value)}
                    onBlur={() => validateField(nodeId)}
                  />
                  <FieldError id={`${nodeId}-error`} message={errors[nodeId]} />
                </div>
                <div className="form-field">
                  <label htmlFor={addressId}>{`Seed ${index + 1} AdminService address`}</label>
                  <Input
                    id={addressId}
                    value={seed.adminAddr}
                    placeholder="127.0.0.1:52001"
                    disabled={pending !== null}
                    aria-invalid={Boolean(errors[addressId] || errors.seeds)}
                    aria-describedby={["connection-seeds-hint", errors.seeds ? "connection-seeds-error" : null, errors[addressId] ? `${addressId}-error` : null].filter(Boolean).join(" ")}
                    onChange={(event) => updateSeed(seed.id, "adminAddr", event.target.value)}
                    onBlur={() => validateField(addressId)}
                  />
                  <FieldError id={`${addressId}-error`} message={errors[addressId]} />
                </div>
                <Button
                  variant="ghost"
                  className="connection-seed-remove"
                  aria-label={`Remove seed ${index + 1}`}
                  disabled={pending !== null}
                  onClick={() => {
                    const seeds = state.seeds.filter((item) => item.id !== seed.id);
                    const next = seeds[Math.min(index, seeds.length - 1)];
                    focusFieldId.current = next ? `seed-${next.id}-node` : "add-admin-seed";
                    setState((current) => ({ ...current, seeds }));
                  }}
                >
                  Remove
                </Button>
              </div>
            );
          })}
        </div>
        <FieldError id="connection-seeds-error" message={errors.seeds} />
        <Button
          id="add-admin-seed"
          className="connection-add-seed"
          variant="secondary"
          disabled={pending !== null}
          onClick={() => {
            const id = nextSeedId.current++;
            focusFieldId.current = `seed-${id}-node`;
            setState((current) => ({ ...current, seeds: [...current.seeds, { id, nodeId: "", adminAddr: "" }] }));
          }}
        >
          Add admin seed
        </Button>
        <div className="form-grid form-grid-two">
          {textField("discovery", "connection-discovery", "Discovery endpoints", { placeholder: "etcd://127.0.0.1:2379" })}
          {textField("clusterName", "connection-cluster-name", "Discovery cluster", { placeholder: "production" })}
        </div>
      </fieldset>

      <fieldset className="connection-section">
        <legend>AdminService security</legend>
        <p className="section-description">TLS files stay on the Console host. Tokens are write-only.</p>
        <div className="form-grid form-grid-two">
          {textField("adminCa", "admin-ca", "Admin CA path", { placeholder: "/etc/rr/ca.pem" })}
          {textField("adminDomain", "admin-domain", "Admin domain", { placeholder: "admin.internal" })}
          {textField("adminCert", "admin-cert", "Admin certificate path", { placeholder: "/etc/rr/client.pem" })}
          {textField("adminKey", "admin-key", "Admin private key path", { placeholder: "/etc/rr/client.key" })}
        </div>
        {editing ? (
          <>
            <SecretModeField
              id="admin-token-mode"
              label="Admin token update"
              value={state.adminTokenMode}
              disabled={pending !== null}
              onChange={(value) => {
                update("adminTokenMode", value);
                setErrors((current) => { const next = { ...current }; delete next.adminToken; return next; });
              }}
            />
            {state.adminTokenMode === "replace" ? textField("adminToken", "new-admin-token", "New admin token", { type: "password", autoComplete: "new-password" }) : null}
          </>
        ) : textField("adminToken", "admin-token", "Admin token", { type: "password", autoComplete: "new-password" })}
      </fieldset>

      <fieldset className="connection-section">
        <legend>Discovery security</legend>
        <div className="form-grid form-grid-two">
          {textField("discoveryCa", "discovery-ca", "Discovery CA path", { placeholder: "/etc/rr/etcd-ca.pem" })}
          {textField("discoveryDomain", "discovery-domain", "Discovery domain", { placeholder: "etcd.internal" })}
          {textField("discoveryCert", "discovery-cert", "Discovery certificate path")}
          {textField("discoveryKey", "discovery-key", "Discovery private key path")}
          {textField("discoveryUsername", "discovery-username", "Discovery username", { autoComplete: "off" })}
        </div>
        {editing ? (
          <>
            <SecretModeField
              id="discovery-password-mode"
              label="Discovery password update"
              value={state.discoveryPasswordMode}
              disabled={pending !== null}
              onChange={(value) => {
                update("discoveryPasswordMode", value);
                setErrors((current) => { const next = { ...current }; delete next.discoveryPassword; return next; });
              }}
            />
            {state.discoveryPasswordMode === "replace" ? textField("discoveryPassword", "new-discovery-password", "New discovery password", { type: "password", autoComplete: "new-password" }) : null}
          </>
        ) : textField("discoveryPassword", "discovery-password", "Discovery password", { type: "password", autoComplete: "new-password" })}
        <label className="checkbox-field">
          <input type="checkbox" checked={state.discoveryAllowInsecure} disabled={pending !== null} onChange={(event) => update("discoveryAllowInsecure", event.target.checked)} />
          Allow insecure discovery transport
        </label>
      </fieldset>

      <details className="connection-advanced" open={advancedOpen} onToggle={(event) => setAdvancedOpen(event.currentTarget.open)}>
        <summary>Advanced settings</summary>
        <div className="advanced-body">
          <div className="form-grid form-grid-two">
            {textField("prometheusUrl", "prometheus-url", "Prometheus URL", { placeholder: "http://127.0.0.1:9090" })}
            {textField("prometheusCluster", "prometheus-cluster", "Prometheus cluster matcher", { placeholder: "production" })}
          </div>
          <label htmlFor="poll-interval">Poll interval (seconds)</label>
          <Input
            id="poll-interval"
            type="number"
            min={1}
            max={300}
            value={state.pollInterval}
            disabled={pending !== null}
            aria-invalid={Boolean(errors.pollInterval)}
            aria-describedby={errors.pollInterval ? "poll-interval-error" : undefined}
            onChange={(event) => update("pollInterval", event.target.value)}
            onBlur={() => validateField("pollInterval")}
          />
          <FieldError id="poll-interval-error" message={errors.pollInterval} />
        </div>
      </details>

      <div className="credential-retention">
        <label className="checkbox-field">
          <input type="checkbox" checked={state.remember} disabled={!keychainAvailable || pending !== null} onChange={(event) => update("remember", event.target.checked)} />
          Remember credentials in OS keychain
        </label>
        <p>{keychainAvailable
          ? "Otherwise, credentials remain in this Console process and must be re-entered after a restart."
          : "OS keychain is unavailable. Credentials remain in this Console process and must be re-entered after a restart."}</p>
      </div>

      {operationError ? (
        <div className="connection-operation-error" role="alert">
          <span>{operationError.message}</span>
          {operationError.correlationId ? <span> Correlation ID: <code>{operationError.correlationId}</code></span> : null}
        </div>
      ) : null}
      {notice ? <div className="connection-operation-success" role="status">{notice}</div> : null}

      <div className="connection-form-actions">
        <Button variant="secondary" disabled={pending !== null} onClick={() => void run("test")}>
          {pending === "test" ? "Testing connection…" : "Test connection"}
        </Button>
        <div className="connection-form-save-actions">
          <Button type="submit" variant="secondary" disabled={pending !== null}>{pending === "save" ? "Saving connection…" : "Save connection"}</Button>
          <Button disabled={pending !== null} onClick={() => void run("connect")}>
            {pending === "connect" ? "Saving & connecting…" : "Save & connect"}
          </Button>
        </div>
      </div>
    </form>
  );
}
