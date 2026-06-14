// Package explorer is the built-in web UI for browsing
// MIDs, DAG structure, peers, and network stats. It is
// served by Mem-Gate at /explorer/*.
//
// The UI is rendered server-side from embedded HTML
// templates. The single page that needs dynamic
// interactivity (the DAG tree) uses vanilla JavaScript
// and fetches node data from /mem/{mid}?format=dag-json.
//
// No external CDN, font, or framework dependencies.
// Everything is in the binary.
package explorer