export interface Task7Envelope {
  meta: Record<string, unknown>;
  data: unknown;
}

export declare function performanceEnvelope(options?: { missingHistory?: boolean }): Task7Envelope;
export declare function snapshotsEnvelope(): Task7Envelope;
export declare function eventsEnvelope(options?: { empty?: boolean }): Task7Envelope;
export declare function sourcesEnvelope(): Task7Envelope;
