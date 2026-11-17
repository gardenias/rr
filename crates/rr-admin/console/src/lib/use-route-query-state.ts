"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { parseQueryState, serializeQueryState, type QueryState, type TimeWindow } from "@/lib/query-state";

export function useRouteQueryState() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const state = parseQueryState(searchParams.toString());
  const replace = (next: QueryState) => {
    const query = serializeQueryState(next);
    router.replace(`${pathname}${query ? `?${query}` : ""}`, { scroll: false });
  };
  return {
    state,
    setWindow: (window: TimeWindow) => replace({ ...state, window }),
    setFilter: (key: keyof QueryState["filters"], value: string) => replace({
      ...state,
      filters: { ...state.filters, [key]: value || undefined },
    }),
  };
}
