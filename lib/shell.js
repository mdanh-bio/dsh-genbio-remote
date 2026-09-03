const SAFE_FIXED_TOKEN_RE = /^[A-Za-z0-9_./:=+@,-]+$/u;

export function shellQuote(value) {
  const text = String(value);
  if (!SAFE_FIXED_TOKEN_RE.test(text)) throw new Error(`unsafe fixed path/token: ${text}`);
  return `'${text}'`;
}

export { SAFE_FIXED_TOKEN_RE };
