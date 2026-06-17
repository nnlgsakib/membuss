import './style.css';

// Minimal DAG tree: walks a DAG by fetching each node's
// children from /mem/{mid}?format=dag-json. Caches the
// fetched nodes in memory to avoid refetching siblings.

(function() {
  "use strict";
  var cache = Object.create(null);

  function el(tag, attrs, children) {
    var e = document.createElement(tag);
    if (attrs) {
      for (var k in attrs) {
        if (k === "class") e.className = attrs[k];
        else if (k === "text") e.textContent = attrs[k];
        else if (k === "data") {
          for (var d in attrs.data) e.dataset[d] = attrs.data[d];
        } else e.setAttribute(k, attrs[k]);
      }
    }
    if (children) {
      for (var i = 0; i < children.length; i++) {
        var c = children[i];
        if (c == null) continue;
        if (typeof c === "string") e.appendChild(document.createTextNode(c));
        else e.appendChild(c);
      }
    }
    return e;
  }

  function fetchNode(mid) {
    if (cache[mid]) return Promise.resolve(cache[mid]);
    return fetch("/mem/" + encodeURIComponent(mid) + "?format=dag-json", { credentials: "same-origin" })
      .then(function(resp) {
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        return resp.json();
      })
      .then(function(j) { cache[mid] = j; return j; });
  }

  function makeRow(mid) {
    var row = el("span", { class: "row" });
    var toggle = el("span", { class: "toggle" });
    toggle.textContent = "[+]";
    var code = el("code", { class: "mid" });
    code.textContent = mid;
    var meta = el("span", { class: "meta" });
    meta.textContent = "";
    row.appendChild(toggle);
    row.appendChild(document.createTextNode(" "));
    row.appendChild(code);
    row.appendChild(meta);
    // Return both the row and the meta so expandNode can
    // update meta.textContent without index-arithmetic
    // into row.children (which is brittle when the row
    // layout changes).
    return { row: row, meta: meta };
  }

  function renderLi(mid, depth) {
    var li = el("li", { class: "node" });
    li.dataset.mid = mid;
    li.dataset.depth = String(depth);
    var built = makeRow(mid);
    li.appendChild(built.row);
    li.appendChild(el("ul", { class: "collapsed" }));
    li._meta = built.meta;
    li._toggle = built.row.children[0];
    return li;
  }

  function expandNode(li) {
    var mid = li.dataset.mid;
    var childUl = li.children[1];
    if (childUl.children.length > 0) {
      // Already expanded; toggle to collapsed.
      childUl.classList.toggle("collapsed", true);
      li._toggle.textContent = "[+]";
      return;
    }
    li._toggle.textContent = "[..]";
    childUl.classList.remove("collapsed");
    childUl.appendChild(el("li", { class: "loading", text: "loading..." }));
    fetchNode(mid).then(function(node) {
      // remove loading
      while (childUl.firstChild) childUl.removeChild(childUl.firstChild);
      var links = (node && node.links) || [];
      li._meta.textContent = "size=" + (node && node.size != null ? node.size : "?") + " children=" + links.length;
      if (links.length === 0) {
        li.classList.add("leaf");
        li._toggle.textContent = " ";
        return;
      }
      li.classList.add("internal");
      li._toggle.textContent = "[-]";
      for (var i = 0; i < links.length; i++) {
        var childLi = renderLi(links[i], parseInt(li.dataset.depth, 10) + 1);
        childUl.appendChild(childLi);
        attachToggle(childLi);
      }
    }).catch(function(err) {
      // remove loading
      while (childUl.firstChild) childUl.removeChild(childUl.firstChild);
      childUl.appendChild(el("li", { class: "error", text: "fetch error: " + err.message }));
      li._toggle.textContent = "[!]";
    });
  }

  function collapseNode(li) {
    var childUl = li.children[1];
    childUl.classList.toggle("collapsed", true);
    li._toggle.textContent = "[+]";
  }

  function attachToggle(li) {
    var row = li.children[0];
    row.addEventListener("click", function() {
      if (li.classList.contains("leaf")) return;
      var childUl = li.children[1];
      if (childUl.classList.contains("collapsed")) expandNode(li);
      else collapseNode(li);
    });
  }

  // Boot: render the root.
  var root = document.getElementById("dag-root");
  if (root) {
    var mid = root.dataset.mid;
    var li = renderLi(mid, 0);
    root.appendChild(li);
    attachToggle(li);
    // Auto-expand root.
    expandNode(li);
  }

  // Clipboard copy buttons (also used on mid.html).
  document.addEventListener("click", function(ev) {
    var t = ev.target;
    if (t && t.classList && t.classList.contains("copy")) {
      var text = t.dataset.copy;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function() {
          var prev = t.textContent;
          t.textContent = "Copied";
          setTimeout(function() { t.textContent = prev; }, 1200);
        });
      } else {
        // Fallback: select the text.
        var range = document.createRange();
        range.selectNode(t.previousElementSibling);
        window.getSelection().removeAllRanges();
        window.getSelection().addRange(range);
      }
    }
  });
})();
