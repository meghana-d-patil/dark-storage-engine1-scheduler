const BASE_SCORES = {
    1: 10,
    2: 20,
    3: 30,
    4: 40
};
export function calculateEffectiveScore(priority, createdAt, now = new Date()) {
    if (!Number.isInteger(priority) || priority < 1 || priority > 4) {
        throw new Error("Invalid priority: must be between 1 and 4");
    }
    const base = BASE_SCORES[priority] ?? 20;
    const createdMs = new Date(createdAt).getTime();
    const waitSeconds = Math.max(0, Math.floor((now.getTime() - createdMs) / 1000));
    const agingBonus = Math.min(30, Math.floor(waitSeconds / 15));
    return base + agingBonus;
}
//# sourceMappingURL=queue.js.map