export declare function envelope<T>(
  pathname: string,
  data: T,
  overrides?: Record<string, unknown>,
): { meta: Record<string, unknown>; data: T };
