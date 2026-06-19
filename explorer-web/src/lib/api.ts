import { base } from '$app/paths';

export async function apiFetch(path: string, init?: RequestInit) {
	const sep = path.includes('?') ? '&' : '?';
	const url = `${base}${path}${sep}format=json`;
	const headers: Record<string, string> = {
		'Accept': 'application/json',
		...Object.fromEntries(new Headers(init?.headers).entries())
	};
	let res = await fetch(url, { ...init, headers });
	// Respect Retry-After on 429 instead of throwing immediately
	if (res.status === 429) {
		const retryAfter = parseInt(res.headers.get('Retry-After') || '2', 10);
		await new Promise(r => setTimeout(r, Math.min(retryAfter, 5) * 1000));
		res = await fetch(url, { ...init, headers: { 'Accept': 'application/json', ...Object.fromEntries(new Headers(init?.headers).entries()) } });
	}
	if (!res.ok) {
		throw new Error(await res.text() || `HTTP ${res.status}`);
	}
	return res.json();
}

export function formatBytes(bytes: number): string {
	if (bytes === 0) return '0 B';
	const k = 1024;
	const sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
	const i = Math.floor(Math.log(bytes) / Math.log(k));
	return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
