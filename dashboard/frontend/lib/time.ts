"use client";

const VIETNAM_TIME_ZONE = "Asia/Ho_Chi_Minh";
const VIETNAM_LOCALE = "vi-VN";

function parseUtcLikeTimestamp(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();
  if (!text) {
    return null;
  }

  const normalized =
    /(?:Z|[+-]\d{2}:\d{2})$/.test(text) || text.includes("GMT") ? text : `${text}Z`;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

export function formatVietnamTimestamp(
  value: unknown,
  fallback = "-"
): string {
  const parsed = parseUtcLikeTimestamp(value);
  if (!parsed) {
    return fallback;
  }
  return new Intl.DateTimeFormat(VIETNAM_LOCALE, {
    timeZone: VIETNAM_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(parsed);
}

export function formatVietnamTimestampLabel(
  label: string,
  value: unknown,
  fallback = "N/A"
): string {
  const formatted = formatVietnamTimestamp(value, fallback);
  return `${label}: ${formatted}`;
}
