/**
 * Read Super-Admin `providerCommissionV1` for margin targets (observe only).
 */
function normalizeCommission(raw) {
    const base = {
        enabled: true,
        defaultPercent: 40,
        byProvider: { telnyx: 40, twilio: 45, pool: 30 },
        byCountry: {},
        byProviderCountry: {},
    };
    if (!raw || typeof raw !== "object") return base;
    if (typeof raw.enabled === "boolean") base.enabled = raw.enabled;
    const d = Number(raw.defaultPercent);
    if (Number.isFinite(d) && d >= 0) base.defaultPercent = d;
    if (raw.byProvider && typeof raw.byProvider === "object") {
        for (const [k, v] of Object.entries(raw.byProvider)) {
            const n = Number(v);
            if (Number.isFinite(n) && n >= 0) base.byProvider[String(k).toLowerCase()] = n;
        }
    }
    if (raw.byCountry && typeof raw.byCountry === "object") {
        for (const [k, v] of Object.entries(raw.byCountry)) {
            const iso = String(k || "").toUpperCase();
            const n = Number(v);
            if (/^[A-Z]{2}$/.test(iso) && Number.isFinite(n)) base.byCountry[iso] = n;
        }
    }
    return base;
}

function resolveCommissionPercent(cfg, { provider, countryIso } = {}) {
    if (!cfg?.enabled) return { percent: 0, source: "disabled" };
    const p = String(provider || "").toLowerCase();
    const iso = String(countryIso || "").toUpperCase();
    if (iso && cfg.byCountry[iso] != null) return { percent: cfg.byCountry[iso], source: "country" };
    if (p && cfg.byProvider[p] != null) return { percent: cfg.byProvider[p], source: "provider" };
    return { percent: cfg.defaultPercent, source: "default" };
}

async function loadProviderCommission(db) {
    try {
        const row = await db.collection("systemsettings").findOne({ key: "providerCommissionV1" });
        return normalizeCommission(row?.value);
    } catch {
        return normalizeCommission(null);
    }
}

module.exports = {
    normalizeCommission,
    resolveCommissionPercent,
    loadProviderCommission,
};
