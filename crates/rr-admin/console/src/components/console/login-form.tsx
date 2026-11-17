"use client";

import { LockKeyhole } from "lucide-react";
import { useSearchParams } from "next/navigation";
import { useState, type FormEvent } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { consoleApi } from "@/lib/api";
import { safeReturnPath } from "@/lib/auth";
import { sanitizeError } from "@/lib/format";

export function LoginForm() {
  const searchParams = useSearchParams();
  const [token, setToken] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const returnPath = safeReturnPath(searchParams.get("next"));

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setPending(true);
    setError(null);
    try {
      await consoleApi.login(token);
      setToken("");
      window.location.assign(returnPath);
    } catch {
      setError("Sign-in failed. Check the viewer credential and try again.");
    } finally {
      setPending(false);
    }
  };

  return (
    <main className="login-page">
      <section className="login-panel" aria-labelledby="login-title">
        <div className="login-brand"><LockKeyhole aria-hidden="true" size={22} /><span>Rr</span></div>
        <div className="page-eyebrow">Consensus control plane</div>
        <h1 id="login-title">Console access</h1>
        <p>Enter the viewer credential issued by the Console operator. It is exchanged for a short-lived session and is never stored by this app.</p>
        <form onSubmit={submit}>
          <label htmlFor="console-token">Viewer credential</label>
          <Input
            id="console-token"
            type="password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            autoComplete="current-password"
            required
            autoFocus
            disabled={pending}
          />
          {error ? <p className="login-error" role="alert">{sanitizeError(error)}</p> : null}
          <Button type="submit" disabled={pending || !token}>{pending ? "Signing in…" : "Sign in to Console"}</Button>
        </form>
        <small>Read-only access · Same-origin session · No browser credential storage</small>
      </section>
    </main>
  );
}
