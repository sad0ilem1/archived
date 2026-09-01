import os
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request
from supabase import create_client, Client

# ============================ CONFIG ============================

TABLE_NAME = "tasks"
ID_COLUMN = "clickup_id"
DELETED_AT_COLUMN = "deleted_at"

OVERWRITE_EXISTING_DELETED_AT = False

SUPABASE_SELECT_BATCH_SIZE = 500
SUPABASE_UPSERT_BATCH_SIZE = 500

REQUEST_DELAY = 0.2  

CLICKUP_VIEW_URL = "https://api.clickup.com/api/v2/view/{view_id}/task"


app = Flask(__name__)


def epoch_ms_para_iso(epoch_ms_str):
    epoch_ms = int(epoch_ms_str)
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def buscar_todas_tarefas_da_view(view_id, headers):
    tarefas = []
    page = 0

    while True:
        url = CLICKUP_VIEW_URL.format(view_id=view_id)
        resp = requests.get(url, headers=headers, params={"page": page}, timeout=30)

        if resp.status_code != 200:
            raise RuntimeError(
                f"Erro ao consultar a view (page={page}): "
                f"status {resp.status_code} - {resp.text}"
            )

        data = resp.json()
        tasks_da_pagina = data.get("tasks", [])

        for t in tasks_da_pagina:
            tarefas.append({"id": t.get("id"), "date_updated": t.get("date_updated")})

        if data.get("last_page", True) or not tasks_da_pagina:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return tarefas


def buscar_ids_existentes_no_banco(supabase: Client, ids):
    existentes = {}
    for i in range(0, len(ids), SUPABASE_SELECT_BATCH_SIZE):
        lote = ids[i:i + SUPABASE_SELECT_BATCH_SIZE]
        resultado = (
            supabase.table(TABLE_NAME)
            .select(f"{ID_COLUMN},{DELETED_AT_COLUMN}")
            .in_(ID_COLUMN, lote)
            .execute()
        )
        for row in resultado.data:
            existentes[row[ID_COLUMN]] = row.get(DELETED_AT_COLUMN)
    return existentes


def atualizar_deleted_at_em_lote(supabase: Client, registros):
    ids_com_sucesso = []
    erros = []

    for i in range(0, len(registros), SUPABASE_UPSERT_BATCH_SIZE):
        lote = registros[i:i + SUPABASE_UPSERT_BATCH_SIZE]
        try:
            supabase.table(TABLE_NAME).upsert(lote, on_conflict=ID_COLUMN).execute()
            ids_com_sucesso.extend([r[ID_COLUMN] for r in lote])
        except Exception as e:
            for r in lote:
                try:
                    (
                        supabase.table(TABLE_NAME)
                        .update({DELETED_AT_COLUMN: r[DELETED_AT_COLUMN]})
                        .eq(ID_COLUMN, r[ID_COLUMN])
                        .execute()
                    )
                    ids_com_sucesso.append(r[ID_COLUMN])
                except Exception as e2:
                    erros.append({"id": r[ID_COLUMN], "motivo": str(e2)})

    return ids_com_sucesso, erros


def executar_sincronizacao():
    clickup_token = os.environ["CLICKUP_API_TOKEN"]
    view_id = os.environ["CLICKUP_VIEW_ID"]
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_KEY"]

    headers = {"Authorization": clickup_token, "accept": "application/json"}
    supabase: Client = create_client(supabase_url, supabase_key)

    tarefas = buscar_todas_tarefas_da_view(view_id, headers)
    tarefas_validas = [t for t in tarefas if t.get("id") and t.get("date_updated")]
    ids_view = [t["id"] for t in tarefas_validas]

    if not ids_view:
        return {"tarefas_na_view": 0, "atualizados": 0, "pulados": 0, "erros": 0}

    existentes_no_banco = buscar_ids_existentes_no_banco(supabase, ids_view)

    pulados = 0
    registros_para_atualizar = []

    for t in tarefas_validas:
        if t["id"] not in existentes_no_banco:
            continue

        deleted_at_atual = existentes_no_banco[t["id"]]
        if deleted_at_atual and not OVERWRITE_EXISTING_DELETED_AT:
            pulados += 1
            continue

        registros_para_atualizar.append({
            ID_COLUMN: t["id"],
            DELETED_AT_COLUMN: epoch_ms_para_iso(t["date_updated"]),
        })

    atualizados, erros = atualizar_deleted_at_em_lote(supabase, registros_para_atualizar)

    return {
        "tarefas_na_view": len(tarefas),
        "encontrados_no_banco": len(existentes_no_banco),
        "atualizados": len(atualizados),
        "pulados": pulados,
        "erros": len(erros),
        "detalhe_erros": erros,
    }


@app.route("/")
def health():
    return "ok"


@app.route("/sync")
def sync():
    chave_esperada = os.environ.get("SYNC_SECRET_KEY")
    chave_recebida = request.args.get("key")

    if not chave_esperada or chave_recebida != chave_esperada:
        return jsonify({"erro": "não autorizado"}), 401

    try:
        resumo = executar_sincronizacao()
        return jsonify(resumo), 200
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


if __name__ == "__main__":
   
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
