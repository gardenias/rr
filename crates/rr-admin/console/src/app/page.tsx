"use client";

import { useEffect } from "react";

export default function IndexPage() {
  useEffect(() => {
    window.location.replace("/connections/");
  }, []);
  return (
    <main className="login-page">
      <p>Opening <a href="/connections/">Rr Console connections</a>…</p>
    </main>
  );
}
