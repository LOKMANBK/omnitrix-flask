"""
Flask Backend – ALNS Optimizasyon Servisi
Server-Sent Events (SSE) ile gerçek zamanlı ilerleme akışı

Endpoints:
  GET  /                → Ana HTML sayfası
  POST /api/solve       → ALNS çözümünü başlat (SSE stream)
  POST /api/solve-sync  → ALNS çözümünü başlat (JSON response)
"""

import json
import queue
import threading
from flask import Flask, request, jsonify, Response, send_from_directory, render_template
from alns_engine import run_alns_engine, convert_to_d, COST_CFG

app = Flask(__name__,
            static_folder='static',
            template_folder='templates')

# 50MB JSON body limiti (büyük D verisi için)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024


@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')


@app.route('/api/solve', methods=['POST'])
def solve_sse():
    """
    ALNS çözümünü SSE (Server-Sent Events) ile çalıştırır.
    Frontend EventSource ile bağlanır ve gerçek zamanlı ilerleme alır.

    Request body: { cfg: {...}, data: {...} }
    SSE events:
      - type: "log"      → { message: "..." }
      - type: "progress"  → { iter, total, bestObj, temp }
      - type: "result"    → { routes, pump_assignments, stats, ... }
      - type: "error"     → { message: "..." }
    """
    body = request.get_json(force=True)
    cfg = body.get('cfg', {})
    data = body.get('data', {})

    # SSE event queue — ALNS thread'den ana thread'e mesaj aktarımı
    event_queue = queue.Queue()

    def on_progress(iteration, total, best_obj, temp):
        event_queue.put(('progress', {
            'iter': iteration,
            'total': total,
            'bestObj': round(best_obj * 10) / 10,
            'temp': round(temp, 4),
        }))

    def on_log(msg):
        event_queue.put(('log', {'message': msg}))

    def run_solver():
        try:
            result = run_alns_engine(cfg, data, on_progress, on_log)

            # Çözümü D formatına dönüştür
            converted = convert_to_d(result['solution'], result['prob'])

            event_queue.put(('result', {
                'routes': converted['routes'],
                'pump_assignments': converted['pump_assignments'],
                'initial_stats': result['initial_stats'],
                'solver4_stats': result['solver4_stats'],
                'final_stats': result['final_stats'],
                'elapsed': result['elapsed'],
            }))
        except Exception as e:
            event_queue.put(('error', {'message': str(e)}))
        finally:
            event_queue.put(('done', None))

    # ALNS'yi ayrı thread'de çalıştır
    solver_thread = threading.Thread(target=run_solver, daemon=True)
    solver_thread.start()

    def generate():
        while True:
            try:
                event_type, payload = event_queue.get(timeout=120)
            except queue.Empty:
                yield f"event: error\ndata: {json.dumps({'message': 'Timeout'})}\n\n"
                break

            if event_type == 'done':
                yield "event: done\ndata: {}\n\n"
                break

            yield f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',  # nginx proxy desteği
        }
    )


@app.route('/api/solve-sync', methods=['POST'])
def solve_sync():
    """
    ALNS çözümünü senkron çalıştırır — tek JSON yanıt.
    SSE kullanamayan istemciler için fallback.
    """
    body = request.get_json(force=True)
    cfg = body.get('cfg', {})
    data = body.get('data', {})

    logs = []

    def on_log(msg):
        logs.append(msg)

    try:
        result = run_alns_engine(cfg, data, on_log=on_log)
        converted = convert_to_d(result['solution'], result['prob'])

        return jsonify({
            'success': True,
            'routes': converted['routes'],
            'pump_assignments': converted['pump_assignments'],
            'initial_stats': result['initial_stats'],
            'solver4_stats': result['solver4_stats'],
            'final_stats': result['final_stats'],
            'elapsed': result['elapsed'],
            'logs': logs,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    print("🧬 ALNS Optimizasyon Servisi başlatılıyor...")
    print("   http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
