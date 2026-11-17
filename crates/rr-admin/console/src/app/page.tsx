"use client";

import { useEffect } from "react";

export default function IndexPage() {
  useEffect(() => {
    window.location.replace("/overview/");
  }, []);
  return (
    <main className="login-page">
      <p>Opening <a href="/overview/">Rr Console overview</a>…</p>
    </main>
  );
}
