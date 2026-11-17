import type { Metadata } from "next";
import { Suspense } from "react";
import { LoginForm } from "@/components/console/login-form";

export const metadata: Metadata = { title: "Sign in" };

export default function LoginPage() {
  return <Suspense fallback={<main className="login-page">Loading sign-in…</main>}><LoginForm /></Suspense>;
}
