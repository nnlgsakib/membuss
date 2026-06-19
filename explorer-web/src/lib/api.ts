import { base } from '$app/paths';

export async function apiFetch(path: string) {
	const sep = path.includes('?') ? '&' : '?';
	const url = `${base}${path}${sep}format=json`;
	const res = await fetch(url, {
		headers: {
			'Accept': 'application/json'
		}
	});
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
