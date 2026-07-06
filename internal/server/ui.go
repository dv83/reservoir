package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/store"
	"reservoir/pkg/logger"
)

const maxDisplayItems = 200

// StartUIServer starts the memory visualization UI server
func StartUIServer(addr string, kvStore store.KVStore, clusterManager *cluster.ClusterManager) *http.Server {
	mux := http.NewServeMux()

	// API Endpoint for memory map
	mux.HandleFunc("/api/memory-map", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Get memory map from store
		memoryMap, err := kvStore.GetMemoryMap()
		if err != nil {
			logger.Error("Failed to get memory map: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Sort by size descending, then by key name for stable ordering
		sort.Slice(memoryMap, func(i, j int) bool {
			if memoryMap[i].Size != memoryMap[j].Size {
				return memoryMap[i].Size > memoryMap[j].Size
			}
			return memoryMap[i].Key < memoryMap[j].Key
		})

		// If more than maxDisplayItems, aggregate the rest into "Others"
		if len(memoryMap) > maxDisplayItems {
			var othersSize int64
			var othersCount int
			for i := maxDisplayItems; i < len(memoryMap); i++ {
				othersSize += memoryMap[i].Size
				othersCount++
			}
			// Truncate to top items
			memoryMap = memoryMap[:maxDisplayItems]
			// Add aggregated "Others" item
			memoryMap = append(memoryMap, store.KeyMemoryInfo{
				Key:      "[Others: " + string(rune('0'+othersCount/100)) + string(rune('0'+(othersCount/10)%10)) + string(rune('0'+othersCount%10)) + " keys]",
				Type:     "other",
				Size:     othersSize,
				TTL:      false,
				Deferred: false,
			})
		}

		// Return as JSON
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(memoryMap); err != nil {
			logger.Error("Failed to encode memory map: %v", err)
		}
	})

	// API Endpoint for cluster nodes
	mux.HandleFunc("/api/nodes", func(w http.ResponseWriter, r *http.Request) {
		if clusterManager == nil {
			http.Error(w, "Cluster mode not enabled", http.StatusServiceUnavailable)
			return
		}

		type NodeResponse struct {
			LocalNode        cluster.NodeInfo         `json:"local_node"`
			Nodes            []cluster.NodeInfo       `json:"nodes"`
			ReplicationStats cluster.ReplicationStats `json:"replication_stats"`
		}

		rawNodes := clusterManager.GetNodes()
		nodesInfo := make([]cluster.NodeInfo, 0, len(rawNodes))
		for _, node := range rawNodes {
			nodesInfo = append(nodesInfo, node.GetInfo())
		}

		resp := NodeResponse{
			LocalNode:        clusterManager.GetLocalNode().GetInfo(),
			Nodes:            nodesInfo,
			ReplicationStats: clusterManager.GetReplicationStats(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	// API Endpoint for CRDT observability: local CRDT state and tombstone
	// gauges, plus this node's anti-entropy activity when clustered.
	mux.HandleFunc("/api/crdt", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		type CRDTResponse struct {
			Store       store.CRDTStats           `json:"store"`
			AntiEntropy *cluster.AntiEntropyStats `json:"anti_entropy,omitempty"`
			Replication *cluster.ReplicationStats `json:"replication,omitempty"`
		}
		resp := CRDTResponse{Store: kvStore.CRDTStats()}
		if clusterManager != nil {
			ae := clusterManager.GetAntiEntropyStats()
			rs := clusterManager.GetReplicationStats()
			resp.AntiEntropy = &ae
			resp.Replication = &rs
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			logger.Error("Failed to encode CRDT stats: %v", err)
		}
	})

	// Serve Static Files (embedded HTML)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		w.Write([]byte(indexHTML))
	})

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("Starting UI server on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("UI server error: %v", err)
		}
	}()

	return server
}

// Embedded HTML for the UI
const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reservoir | Memory Visualization</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-deep: #0a0a0c;
            --bg-card: #16161a;
            --bg-header: rgba(18, 18, 22, 0.8);
            --accent: #3b82f6;
            --accent-glow: rgba(59, 130, 246, 0.5);
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-dim: #64748b;
            --border: rgba(255, 255, 255, 0.08);
            --header-h: 64px;
            --footer-h: 32px;
            --glass: blur(12px) saturate(180%);
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Inter', system-ui, -apple-system, sans-serif; 
            background: var(--bg-deep); 
            color: var(--text-primary); 
            overflow: hidden;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }

        /* Header Styling */
        header {
            height: var(--header-h);
            background: var(--bg-header);
            backdrop-filter: var(--glass);
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 24px;
            z-index: 1000;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
            font-weight: 700;
            font-size: 20px;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #fff 0%, #94a3b8 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .logo-icon {
            width: 32px;
            height: 32px;
            background: var(--accent);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 0 15px var(--accent-glow);
        }

        nav {
            display: flex;
            gap: 4px;
        }

        .nav-tab {
            padding: 8px 16px;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 500;
            color: var(--text-secondary);
            cursor: pointer;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            border: 1px solid transparent;
        }

        .nav-tab:hover {
            color: var(--text-primary);
            background: rgba(255, 255, 255, 0.05);
        }

        .nav-tab.active {
            color: var(--text-primary);
            background: rgba(59, 130, 246, 0.1);
            border-color: rgba(59, 130, 246, 0.2);
            box-shadow: inset 0 0 10px rgba(59, 130, 246, 0.05);
        }

        /* Main Content Area */
        main {
            flex: 1;
            position: relative;
            overflow: hidden;
        }

        .tab-content {
            position: absolute;
            top: 0; left: 0; width: 100%; height: 100%;
            display: none;
            animation: fadeIn 0.3s ease;
        }

        .tab-content.active { display: block; }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(4px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* Memory Map Tab */
        #canvas { display: block; width: 100%; height: 100%; }

        #tooltip { 
            position: absolute; 
            background: rgba(15, 15, 20, 0.95); 
            backdrop-filter: blur(8px);
            padding: 12px; 
            border-radius: 12px; 
            pointer-events: none; 
            display: none; 
            font-size: 12px; 
            z-index: 2000; 
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
            line-height: 1.5;
        }

        .controls { 
            position: absolute; 
            top: 20px; 
            right: 24px; 
            background: rgba(22, 22, 26, 0.7); 
            backdrop-filter: var(--glass);
            padding: 8px 16px; 
            border-radius: 12px; 
            display: flex; 
            align-items: center;
            gap: 16px; 
            border: 1px solid var(--border);
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        }

        .btn-refresh {
            background: var(--accent);
            color: #fff;
            border: none;
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.1s;
        }
        .btn-refresh:active { transform: scale(0.95); }

        .stat { font-family: 'JetBrains Mono', monospace; font-size: 13px; }
        .stat-label { color: var(--text-dim); margin-right: 4px; }

        .legend { 
            position: absolute; 
            bottom: 24px; 
            right: 24px; 
            background: rgba(22, 22, 26, 0.7); 
            backdrop-filter: var(--glass);
            padding: 12px; 
            border-radius: 12px; 
            border: 1px solid var(--border);
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .legend-item { display: flex; align-items: center; gap: 8px; font-size: 11px; font-weight: 500; color: var(--text-secondary); }
        .color-box { width: 12px; height: 12px; border-radius: 3px; }

        /* Footer Styling */
        footer {
            height: var(--footer-h);
            background: var(--bg-card);
            border-top: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 16px;
            font-size: 11px;
            color: var(--text-dim);
            font-family: 'JetBrains Mono', monospace;
        }

        .status-pill {
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .status-dot {
            width: 6px;
            height: 6px;
            background: #10b981;
            border-radius: 50%;
            box-shadow: 0 0 8px #10b981;
        }

        /* Nodes Tab Styling */
        .nodes-grid {
            padding: 24px;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .card-label { font-size: 11px; font-weight: 700; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.05em; }
        .card-value { font-size: 24px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }

        .nodes-table-container { 
            margin: 0 24px 24px 24px; 
            background: var(--bg-card); 
            border: 1px solid var(--border); 
            border-radius: 16px; 
            overflow: hidden; 
        }

        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th { background: rgba(255,255,255,0.03); text-align: left; padding: 12px 16px; color: var(--text-dim); font-weight: 600; border-bottom: 1px solid var(--border); }
        td { padding: 12px 16px; border-bottom: 1px solid var(--border); color: var(--text-secondary); }
        tr:last-child td { border-bottom: none; }
        tr:hover td { background: rgba(255,255,255,0.02); }

        .badge { padding: 4px 8px; border-radius: 6px; font-size: 10px; font-weight: 700; text-transform: uppercase; }
        .badge-leader { background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }
        .badge-follower { background: rgba(148, 163, 184, 0.1); color: var(--text-secondary); border: 1px solid var(--border); }
        .badge-alive { background: rgba(16, 185, 129, 0.2); color: #34d399; }
        .badge-offline { background: rgba(239, 68, 68, 0.2); color: #f87171; }
    </style>
</head>
<body>
    <header>
        <div class="brand">
            <div class="logo-icon">R</div>
            <span>Reservoir</span>
        </div>
        <nav>
            <div class="nav-tab active" data-tab="memory">Memory Map</div>
            <div class="nav-tab" data-tab="nodes">Nodes</div>
            <div class="nav-tab" data-tab="logs">Logs</div>
            <div class="nav-tab" data-tab="config">Config</div>
        </nav>
        <div style="width: 120px;"></div> <!-- Spacer -->
    </header>

    <main>
        <div id="memory-tab" class="tab-content active">
            <div id="loading" style="display: none;">Loading...</div>
            <div id="tooltip"></div>
            <div class="controls">
                <div class="stat"><span class="stat-label">SIZE</span> <span id="total-size">0 B</span></div>
                <button class="btn-refresh" onclick="fetchData()">Refresh</button>
            </div>
            <div class="legend">
                <div class="legend-item"><div class="color-box" style="background: #3498db;"></div>String</div>
                <div class="legend-item"><div class="color-box" style="background: #2ecc71;"></div>List</div>
                <div class="legend-item"><div class="color-box" style="background: #e67e22;"></div>Set</div>
                <div class="legend-item"><div class="color-box" style="background: #9b59b6;"></div>Hash</div>
                <div class="legend-item"><div class="color-box" style="background: repeating-linear-gradient(45deg, rgba(127,140,141,0.5), rgba(127,140,141,0.5) 5px, rgba(149,165,166,0.5) 5px, rgba(149,165,166,0.5) 10px);"></div>Deferred</div>
                <div class="legend-item"><div class="color-box" style="background: rgba(255,255,255,0.4);"></div>TTL</div>
            </div>
            <canvas id="canvas"></canvas>
        </div>

        <div id="nodes-tab" class="tab-content" style="overflow-y: auto;">
            <div class="nodes-grid">
                <div class="card">
                    <div class="card-label">Cluster ID</div>
                    <div class="card-value" id="cluster-id" style="font-size: 14px; color: var(--text-dim);">---</div>
                </div>
                <div class="card">
                    <div class="card-label">Overall Nodes</div>
                    <div class="card-value" id="nodes-count">0</div>
                </div>
                <div class="card">
                    <div class="card-label">Replication Queue</div>
                    <div class="card-value" id="repl-queue">0</div>
                </div>
                <div class="card">
                    <div class="card-label">Dropped Events</div>
                    <div class="card-value" id="dropped-events" style="color: #f87171;">0</div>
                </div>
            </div>
            
            <div class="nodes-table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Node ID</th>
                            <th>Role</th>
                            <th>Status</th>
                            <th>Address</th>
                            <th>Last Seen</th>
                        </tr>
                    </thead>
                    <tbody id="nodes-table-body">
                        <!-- Rows added via JS -->
                    </tbody>
                </table>
            </div>
        </div>

        <div id="logs-tab" class="tab-content">
            <div class="placeholder-content">
                <p>Logs tab content coming soon...</p>
            </div>
        </div>

        <div id="config-tab" class="tab-content">
            <div class="placeholder-content">
                <p>Server configuration details coming soon...</p>
            </div>
        </div>
    </main>

    <footer>
        <div class="status-pill">
            <div class="status-dot"></div>
            <span>CONNECTED TO ENGINE</span>
        </div>
        <div>RESERVOIR v1.2.0-STABLE</div>
        <div id="server-uptime">UPTIME: --:--:--</div>
    </footer>

    <script>
        // DOM Elements
        const canvas = document.getElementById('canvas');
        const ctx = canvas.getContext('2d');
        const tooltip = document.getElementById('tooltip');
        const tabs = document.querySelectorAll('.nav-tab');
        const contents = document.querySelectorAll('.tab-content');
        
        let width, height;
        let nodes = [];
        let startTime = Date.now();

        // Layout Handling
        function resize() {
            const main = document.querySelector('main');
            width = main.clientWidth;
            height = main.clientHeight;
            canvas.width = width;
            canvas.height = height;
            if (document.getElementById('memory-tab').classList.contains('active')) {
                draw();
            }
        }
        window.addEventListener('resize', resize);

        // Tab Switching
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                const target = tab.dataset.tab;
                tabs.forEach(t => t.classList.remove('active'));
                contents.forEach(c => c.classList.remove('active'));
                
                tab.classList.add('active');
                document.getElementById(target + '-tab').classList.add('active');
                
                if (target === 'memory') {
                    resize();
                    fetchData();
                } else if (target === 'nodes') {
                    fetchNodes();
                }
            });
        });

        const colors = {
            'string': '#3498db',
            'list':   '#2ecc71',
            'set':    '#e67e22',
            'hash':   '#9b59b6',
            'unknown': '#52525b'
        };

        function getTypeColor(type, ttl) {
            let base = colors[type.toLowerCase()] || colors['unknown'];
            if (ttl) {
                return base + '66'; // ~40% opacity hex
            }
            return base;
        }

        async function fetchData() {
            if (!document.getElementById('memory-tab').classList.contains('active')) return;
            try {
                const res = await fetch('/api/memory-map');
                const data = await res.json();
                
                // Sort handles on backend, but stable sorting here as well
                data.sort((a, b) => b.size - a.size || a.key.localeCompare(b.key));
                
                calculateTreemap(data);
                
                const totalBytes = data.reduce((acc, item) => acc + item.size, 0);
                document.getElementById('total-size').textContent = formatBytes(totalBytes);
            } catch (e) {
                console.error("Fetch error details:", e);
            }
        }

        async function fetchNodes() {
            if (!document.getElementById('nodes-tab').classList.contains('active')) return;
            try {
                const res = await fetch('/api/nodes');
                const data = await res.json();
                
                document.getElementById('cluster-id').textContent = data.local_node.cluster_id;
                document.getElementById('nodes-count').textContent = data.nodes.length;
                document.getElementById('repl-queue').textContent = data.replication_stats.queue_size;
                document.getElementById('dropped-events').textContent = data.replication_stats.dropped_event_count;

                const tbody = document.getElementById('nodes-table-body');
                tbody.innerHTML = '';
                
                data.nodes.forEach(node => {
                    const row = document.createElement('tr');
                    const isLocal = node.node_id === data.local_node.node_id;
                    const lastSeen = isLocal ? 'Now' : formatTime(node.last_heartbeat);

                    row.innerHTML =
                        '<td>' + escapeHtml(node.node_id) + (isLocal ? ' <span style="color:var(--text-dim)">(Local)</span>' : '') + '</td>' +
                        '<td><span class="badge ' + (node.is_leader ? 'badge-leader' : 'badge-follower') + '">' + (node.is_leader ? 'Leader' : 'Follower') + '</span></td>' +
                        '<td><span class="badge ' + (node.is_alive ? 'badge-alive' : 'badge-offline') + '">' + (node.is_alive ? 'Alive' : 'Offline') + '</span></td>' +
                        '<td>' + escapeHtml(node.address) + ':' + escapeHtml(node.port) + '</td>' +
                        '<td>' + escapeHtml(lastSeen) + '</td>';
                    tbody.appendChild(row);
                });
            } catch (e) {
                console.error("Nodes fetch error:", e);
            }
        }

        // escapeHtml prevents stored XSS: key names, node IDs and addresses are
        // fully attacker-controlled (any client can SET an arbitrary key name),
        // so they must be escaped before being placed into innerHTML.
        function escapeHtml(s) {
            return String(s)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        function formatTime(isoStr) {
            const date = new Date(isoStr);
            return date.toLocaleTimeString();
        }

        function calculateTreemap(data) {
            if (!data || data.length === 0) return;
            const totalSize = data.reduce((acc, item) => acc + item.size, 0);
            nodes = [];
            layout(data, 0, 0, width, height, totalSize);
            draw();
        }

        function layout(items, x, y, w, h, totalSize) {
            if (items.length === 0) return;
            if (items.length === 1) {
                nodes.push({ ...items[0], x, y, w, h });
                return;
            }

            let mid = 0;
            let currentSize = 0;
            const halfSize = totalSize / 2;
            
            for (let i = 0; i < items.length; i++) {
                currentSize += items[i].size;
                mid = i;
                if (currentSize >= halfSize) break;
            }
            
            const leftItems = items.slice(0, mid + 1);
            const rightItems = items.slice(mid + 1);
            const leftSize = currentSize;
            const rightSize = totalSize - leftSize;

            if (w > h) {
                const leftW = (leftSize / totalSize) * w;
                layout(leftItems, x, y, leftW, h, leftSize);
                layout(rightItems, x + leftW, y, w - leftW, h, rightSize);
            } else {
                const topH = (leftSize / totalSize) * h;
                layout(leftItems, x, y, w, topH, leftSize);
                layout(rightItems, x, y + topH, w, h - topH, rightSize);
            }
        }

        function draw() {
            ctx.clearRect(0, 0, width, height);
            
            nodes.forEach(node => {
                ctx.fillStyle = getTypeColor(node.type, node.ttl);
                ctx.fillRect(node.x, node.y, node.w, node.h);
                
                ctx.strokeStyle = 'rgba(0,0,0,0.1)';
                ctx.lineWidth = 1;
                ctx.strokeRect(node.x, node.y, node.w, node.h);

                if (node.deferred) {
                    ctx.save();
                    ctx.beginPath();
                    ctx.rect(node.x, node.y, node.w, node.h);
                    ctx.clip();
                    ctx.strokeStyle = 'rgba(255,255,255,0.2)';
                    ctx.lineWidth = 1.5;
                    for (let i = -node.h; i < node.w; i += 8) {
                        ctx.moveTo(node.x + i, node.y);
                        ctx.lineTo(node.x + i + node.h, node.y + node.h);
                    }
                    ctx.stroke();
                    ctx.restore();
                }

                if (node.w > 50 && node.h > 24) {
                    ctx.fillStyle = 'rgba(255,255,255,0.95)';
                    ctx.font = '600 11px Inter, sans-serif';
                    let label = node.key;
                    if (ctx.measureText(label).width > node.w - 10) {
                        label = label.substring(0, 10) + '...';
                    }
                    ctx.fillText(label, node.x + 8, node.y + 18);
                }
            });
        }

        // Interaction
        canvas.addEventListener('mousemove', e => {
            const rect = canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            const node = nodes.find(n => x >= n.x && x <= n.x + n.w && y >= n.y && y <= n.y + n.h);
            
            if (node) {
                tooltip.style.display = 'block';
                tooltip.style.left = e.clientX + 16 + 'px';
                tooltip.style.top = e.clientY + 12 + 'px';
                tooltip.innerHTML =
                    '<div style="font-weight:700;margin-bottom:4px;color:#fff">' + escapeHtml(node.key) + '</div>' +
                    '<div style="display:grid;grid-template-columns:auto 1fr;gap:4px 12px">' +
                    '<span class="stat-label">TYPE</span> <span>' + escapeHtml(node.type) + '</span>' +
                    '<span class="stat-label">SIZE</span> <span>' + formatBytes(node.size) + '</span>' +
                    '<span class="stat-label">TTL</span> <span>' + (node.ttl ? 'Active' : 'No') + '</span>' +
                    '<span class="stat-label">DEFERRED</span> <span>' + (node.deferred ? 'Pending' : 'No') + '</span>' +
                    '</div>';
            } else {
                tooltip.style.display = 'none';
            }
        });

        function formatBytes(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        // Uptime Timer
        setInterval(() => {
            const sec = Math.floor((Date.now() - startTime) / 1000);
            const h = Math.floor(sec / 3600).toString().padStart(2, '0');
            const m = Math.floor((sec % 3600) / 60).toString().padStart(2, '0');
            const s = (sec % 60).toString().padStart(2, '0');
            document.getElementById('server-uptime').textContent = 'UPTIME: ' + h + ':' + m + ':' + s;
        }, 1000);

        // Global Auto-refresh
        setInterval(() => {
            if (document.getElementById('memory-tab').classList.contains('active')) {
                fetchData();
            } else if (document.getElementById('nodes-tab').classList.contains('active')) {
                fetchNodes();
            }
        }, 2000);

        // Initialization
        resize();
        fetchData();
    </script>
</body>
</html>`
