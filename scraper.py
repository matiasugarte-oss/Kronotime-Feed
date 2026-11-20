"""
Scraper de productos para tiendas Shopify (pensado para https://kronotime.com).
Misma estructura y salidas que el feed de Shaka.

Estrategia:
  1. Lee el catálogo completo desde /products.json (250 productos por página): nombre,
     descripción, marca, imágenes y, por variante, precio, precio tachado y disponibilidad.
     Si ese endpoint estuviera bloqueado, usa el sitemap + /products/<handle>.js.
  2. Completa el código de barras (GTIN) de cada variante desde /products/<handle>.js.
     Se guarda en caché en feed.json, así solo se consulta para productos nuevos.
  3. Escribe feed.json, feed.csv, feed.xml (formato de referencia) y feed_google.xml (completo).

Uso:  pip install requests
      python scraper.py
"""
import csv
import html
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

BASE = "https://kronotime.com"
DELAY = 0.5           # pausa de cada worker entre requests (ser amable con el servidor)
WORKERS = 3           # requests en paralelo (solo para los .js de códigos de barras)
TIMEOUT = 20
TIEMPO_MAX = 13 * 60  # corta y guarda lo que tenga, para no pasarse del límite de GitHub
MAX_PAGES = 40        # tope de seguridad: 40 x 250 = 10.000 productos
MIN_RATIO = 0.7       # si hoy aparecen menos del 70% de los productos de ayer, no se pisa el feed

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; ProductFeedBot/1.0)",
    "Accept-Language": "es-CO,es;q=0.9",
})

ESTADOS = {}
INICIO = time.time()
FRENO = {"hasta": 0.0}   # si la tienda responde 429, TODOS los workers esperan hasta este instante


def get(url, retries=6):
    """GET con reintentos. Ante 429/503 (límite de consultas) espera con backoff y respeta Retry-After."""
    for intento in range(retries):
        if time.time() - INICIO > TIEMPO_MAX + 90:
            return None
        espera = FRENO["hasta"] - time.time()
        if espera > 0:
            time.sleep(espera)
        try:
            r = session.get(url, timeout=TIMEOUT)
            ESTADOS[r.status_code] = ESTADOS.get(r.status_code, 0) + 1
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None
            if r.status_code in (429, 403, 503):
                ra = r.headers.get("Retry-After", "")
                pausa = min(int(float(ra)) if re.fullmatch(r"\d+(\.\d+)?", ra) else 8 * (intento + 1), 60)
                FRENO["hasta"] = max(FRENO["hasta"], time.time() + pausa)
                print(f"  HTTP {r.status_code} en {url} -> pausa {pausa}s (intento {intento + 1}/{retries})", flush=True)
                continue
        except requests.RequestException as e:
            ESTADOS["error"] = ESTADOS.get("error", 0) + 1
            print(f"  error {type(e).__name__} en {url}", flush=True)
        time.sleep(2 * (intento + 1))
    return None


# ---------- 1. Leer el catálogo ----------

def a_precio(valor, en_centavos=False):
    """'349900.00' -> 349900.0 | 34990000 (centavos, formato .js) -> 349900.0"""
    if valor in (None, ""):
        return None
    try:
        n = float(valor)
    except (TypeError, ValueError):
        return None
    return n / 100 if en_centavos else n


def limpiar_html(texto):
    texto = re.sub(r"<(br|/p|/li|/h\d)[^>]*>", " ", texto or "", flags=re.I)
    texto = re.sub(r"<[^>]+>", " ", texto)
    return re.sub(r"\s+", " ", html.unescape(texto)).strip()


def normalizar(p, en_centavos=False):
    """Pasa un producto de Shopify (products.json o .js) al formato interno del feed."""
    imagenes, img_por_variante = [], {}
    for img in p.get("images") or []:
        src = img.get("src") if isinstance(img, dict) else img
        if not src:
            continue
        src = "https:" + src if src.startswith("//") else src
        if src not in imagenes:
            imagenes.append(src)
        for vid in (img.get("variant_ids") or []) if isinstance(img, dict) else []:
            img_por_variante[vid] = src

    opciones = [o.get("name", "") if isinstance(o, dict) else str(o) for o in p.get("options") or []]
    tags = p.get("tags") or []
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]

    variantes = []
    for v in p.get("variants") or []:
        fi = v.get("featured_image")
        img = (fi.get("src") if isinstance(fi, dict) else None) or img_por_variante.get(v.get("id"))
        variantes.append({
            "variant_id": v.get("id"),
            "sku": v.get("sku"),
            "barcode": v.get("barcode"),          # products.json no lo trae; se completa en el paso 2
            "opciones": "" if v.get("title") in (None, "Default Title") else v.get("title"),
            "opciones_nombres": opciones,
            "precio": a_precio(v.get("price"), en_centavos),
            "precio_tachado": a_precio(v.get("compare_at_price"), en_centavos),
            "disponible": bool(v.get("available")),
            "imagen": ("https:" + img if img and img.startswith("//") else img),
        })

    return {
        "product_id": p.get("id"),
        "handle": p.get("handle"),
        "url": f"{BASE}/products/{p.get('handle')}",
        "nombre": (p.get("title") or "").strip(),
        "marca": (p.get("vendor") or "").strip(),
        "tipo": (p.get("product_type") or p.get("type") or "").strip(),
        "tags": tags,
        "descripcion": limpiar_html(p.get("body_html") or p.get("description") or ""),
        "imagenes": imagenes,
        "variantes": variantes,
    }


def catalogo_products_json():
    productos = []
    for page in range(1, MAX_PAGES + 1):
        r = get(f"{BASE}/products.json?limit=250&page={page}")
        if not r:
            break
        try:
            lote = r.json().get("products", [])
        except ValueError:
            break
        if not lote:
            break
        productos += [normalizar(p) for p in lote]
        print(f"  products.json página {page}: {len(lote)} productos", flush=True)
        time.sleep(DELAY)
    return productos


def handles_desde_sitemap():
    handles, pendientes, vistos = set(), [f"{BASE}/sitemap.xml"], set()
    while pendientes:
        sm = pendientes.pop()
        if sm in vistos:
            continue
        vistos.add(sm)
        r = get(sm)
        if not r:
            continue
        for loc in re.findall(r"<loc>\s*(.*?)\s*</loc>", r.text):
            loc = html.unescape(loc)
            if ".xml" in loc and "sitemap" in loc:
                pendientes.append(loc)
            else:
                m = re.match(rf"{re.escape(BASE)}/products/([^/?#]+)$", loc)
                if m:
                    handles.add(m.group(1))
    return handles


def producto_js(handle):
    r = get(f"{BASE}/products/{handle}.js")
    time.sleep(DELAY)
    if not r:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def catalogo_sitemap():
    handles = sorted(handles_desde_sitemap())
    print(f"Sitemap: {len(handles)} productos", flush=True)
    productos = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for i, p in enumerate(pool.map(producto_js, handles), 1):
            if p:
                productos.append(normalizar(p, en_centavos=True))
            if i % 100 == 0:
                print(f"  {i}/{len(handles)} procesados ({time.time() - INICIO:.0f}s)", flush=True)
    return productos


# ---------- 2. Códigos de barras (GTIN), con caché ----------

def gtin_valido(codigo):
    codigo = re.sub(r"\D", "", str(codigo or ""))
    if len(codigo) not in (8, 12, 13, 14) or set(codigo) == {"0"}:
        return None
    digitos = [int(d) for d in codigo]
    control = digitos.pop()
    suma = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(digitos)))
    return codigo if (10 - suma % 10) % 10 == control else None


def completar_barcodes(productos, previos):
    cache = {v["variant_id"]: v.get("barcode") for p in previos.values() for v in p["variantes"]
             if v.get("barcode") is not None}
    pendientes = []
    for p in productos:
        for v in p["variantes"]:
            if v.get("barcode") is None and v["variant_id"] in cache:
                v["barcode"] = cache[v["variant_id"]]
        if any(v.get("barcode") is None for v in p["variantes"]):
            pendientes.append(p)
    if not pendientes:
        return
    print(f"Buscando códigos de barras de {len(pendientes)} productos nuevos...", flush=True)

    def tarea(p):
        if time.time() - INICIO > TIEMPO_MAX:
            return p, None
        return p, producto_js(p["handle"])

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for i, (p, js) in enumerate(pool.map(tarea, pendientes), 1):
            if js:
                codigos = {v.get("id"): (v.get("barcode") or "") for v in js.get("variants") or []}
                for v in p["variantes"]:
                    v["barcode"] = codigos.get(v["variant_id"], "")   # "" = consultado, sin código
            if i % 100 == 0:
                print(f"  {i}/{len(pendientes)} ({time.time() - INICIO:.0f}s)", flush=True)


# ---------- 3. Feed XML para Google Merchant Center ----------

MONEDA = "COP"
EDAD = "adult"
INCLUIR_SIN_STOCK = False   # solo afecta a feed_google.xml: False = se omiten los productos con TODAS las
                            # variantes agotadas. feed.xml siempre incluye el catálogo completo.

# Mismos 4 parámetros UTM, en el mismo orden, que el feed de Shaka / referencia (Uniwork)
UTM = "utm_source=Google%20Shopping&utm_campaign=feed%20torem&utm_medium=cpc&utm_term=adtribes"

# (palabras clave en nombre/tipo, product_type, categoría de Google)
CATEGORIAS = [
    (["CORREA", "PULSO", "MALLA", "REPUESTO"], "Correas",
     "Apparel & Accessories > Jewelry > Watch Accessories > Watch Bands"),
    (["RELOJ", "RELOJES", "SMARTWATCH", "WATCH"], "Relojes",
     "Apparel & Accessories > Jewelry > Watches"),
    (["GAFAS", "GAFA", "LENTES", "SUNGLASSES", "EYEWEAR"], "Gafas de sol",
     "Apparel & Accessories > Clothing Accessories > Sunglasses"),
    (["PULSERA", "BRAZALETE"], "Joyería",
     "Apparel & Accessories > Jewelry > Bracelets"),
    (["COLLAR", "CADENA"], "Joyería", "Apparel & Accessories > Jewelry > Necklaces"),
    (["ARETES", "ARETE", "TOPOS"], "Joyería", "Apparel & Accessories > Jewelry > Earrings"),
    (["ANILLO"], "Joyería", "Apparel & Accessories > Jewelry > Rings"),
    (["BILLETERA"], "Accesorios", "Apparel & Accessories > Handbags, Wallets & Cases > Wallets & Money Clips"),
]
CATEGORIA_DEFECTO = ("Accesorios", "Apparel & Accessories > Jewelry")

# Marcas y palabras que se escriben siempre igual
FORMA_FIJA = {"G-SHOCK": "G-Shock", "BOSS": "BOSS", "Q&Q": "Q&Q", "CASÍO": "Casio",
              "TECHNOMARINE": "TechnoMarine", "EDIFICE": "Edifice"}
MINUSCULAS = {"para", "de", "del", "y", "con", "en", "la", "el", "a", "e"}


def clasificar(p):
    palabras = set(re.findall(r"[A-ZÁÉÍÓÚÜÑ]+", f"{p['nombre']} {p['tipo']}".upper()))
    for claves, tipo, cat in CATEGORIAS:
        if palabras & set(claves):
            return tipo, cat
    return CATEGORIA_DEFECTO


def genero(p):
    texto = f"{p['nombre']} {' '.join(p['tags'])}".upper()
    if "UNISEX" in texto:
        return "unisex"
    hombre, mujer = "HOMBRE" in texto, "MUJER" in texto or "DAMA" in texto
    if hombre and not mujer:
        return "male"
    if mujer and not hombre:
        return "female"
    return "unisex"


def formato_palabra(palabra, primera):
    if palabra.upper() in FORMA_FIJA:
        return FORMA_FIJA[palabra.upper()]
    if any(c.isdigit() for c in palabra):          # códigos de modelo: siempre en mayúsculas
        return palabra.upper()
    if len(palabra) <= 3 and not re.search(r"[AEIOUÁÉÍÓÚaeiouáéíóú]", palabra):
        return palabra.upper()                     # siglas sin vocales: TH, LTP, CK...
    if not primera and palabra.lower() in MINUSCULAS:
        return palabra.lower()
    return palabra[:1].upper() + palabra[1:].lower()


def limpiar_nombre(nombre):
    """'RELOJ PARA MUJER CASIO CASIO LTP-V005D-1AUDF - PLATEADO' ->
       'Reloj para Mujer Casio LTP-V005D-1AUDF - Plateado'"""
    nombre = re.sub(r"\s+", " ", nombre).strip(" -")
    salida, anterior = [], None
    for i, palabra in enumerate(nombre.split(" ")):
        if anterior and palabra.upper() == anterior.upper() and palabra != "-":
            continue                                  # 'CASIO CASIO', 'F-91W-1D F-91W-1D'
        salida.append(palabra if palabra == "-" else formato_palabra(palabra, i == 0))
        anterior = palabra
    return " ".join(salida)


def extraer_color(nombre):
    """Lo que viene después del último ' - ' en el título: 'Oro, Acero', 'Plateado'..."""
    if " - " not in nombre:
        return None
    color = nombre.rsplit(" - ", 1)[1].strip()
    return color.replace(", ", "/")[:100] if color and not any(c.isdigit() for c in color) else None


def precio_fmt(n):
    return f"{float(n):.2f} {MONEDA}"


CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def esc(texto):
    from xml.sax.saxutils import escape
    return escape(CTRL.sub("", str(texto)))


def linea_xml(k, val):
    if k == "g:description":   # CDATA, como en el feed de referencia
        return f"      <{k}><![CDATA[{CTRL.sub('', str(val)).replace(']]>', ']]&gt;')}]]></{k}>"
    return f"      <{k}>{esc(val)}</{k}>"


def item_xml(campos):
    lineas = [linea_xml(k, val) for k, val in campos if val]
    return "    <item>\n" + "\n".join(lineas) + "\n    </item>"


def escribir_rss(items, ruta):
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0" xmlns:g="http://base.google.com/ns/1.0">\n'
        "  <channel>\n"
        "    <title>Kronotime</title>\n"
        f"    <link>{BASE}</link>\n"
        "    <description>Feed de productos de Kronotime</description>\n"
        + "\n".join(items) +
        "\n  </channel>\n</rss>\n"
    )
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(xml)


def generar_google_xml(productos, ruta):
    items, items_ref, omitidos = [], [], 0
    for p in productos:
        nombre_orig = p["nombre"]
        if not nombre_orig or "GIFT" in nombre_orig.upper() or "TARJETA DE REGALO" in nombre_orig.upper():
            continue  # las gift cards no se pueden publicitar en Shopping
        nombre = limpiar_nombre(nombre_orig)
        marca = FORMA_FIJA.get(p["marca"].upper(), p["marca"].title()) if p["marca"] else None
        tipo, categoria = clasificar(p)
        gen = genero(p)
        color = extraer_color(nombre)
        product_type = f"Home > {tipo}" + (f" > {marca}" if marca else "")

        desc = p["descripcion"]
        if len(desc) < 30:
            desc = f"{nombre}. {tipo[:-1] if tipo.endswith('s') else tipo} original {marca or ''} " \
                   f"con garantía. Compra online en Kronotime con envío a toda Colombia.".replace("  ", " ")

        variantes = [v for v in p["variantes"] if v.get("precio")]
        agotado_total = all(not v["disponible"] for v in variantes)
        publicar_google = INCLUIR_SIN_STOCK or not agotado_total
        if not publicar_google:
            omitidos += 1

        for v in variantes:
            precio_actual, tachado = v["precio"], v.get("precio_tachado")
            opcion = (v.get("opciones") or "").strip()
            titulo = nombre
            if opcion and opcion.upper() not in nombre.upper():   # la talla/opción va antes del color
                base, sep, col = nombre.rpartition(" - ") if color else (nombre, "", "")
                titulo = f"{base} {opcion}{sep}{col}"

            link_variante = f"{p['url']}?variant={v['variant_id']}"
            link = f"{link_variante}&{UTM}"

            imagen = v.get("imagen") or (p["imagenes"][0] if p["imagenes"] else None)
            if not imagen:
                continue  # Google rechaza productos sin imagen
            extras = [i for i in p["imagenes"] if i != imagen][:10]

            disp = "in_stock" if v["disponible"] else "out_of_stock"
            gtin = gtin_valido(v.get("barcode"))
            mpn = (v.get("sku") or "").strip() or None
            # Identificadores: GTIN si es válido; si no, marca + MPN; si tampoco, identifier_exists = no
            if gtin:
                ids = [("g:gtin", gtin), ("g:mpn", mpn)]
            elif marca and mpn:
                ids = [("g:mpn", mpn)]
            else:
                ids = [("g:identifier_exists", "no")]

            campos = [
                ("g:id", v["variant_id"]),
                ("g:title", titulo[:150]),
                ("g:description", desc[:5000]),
                ("g:link", link),
                ("g:image_link", imagen),
                *[("g:additional_image_link", i) for i in extras],
                ("g:availability", disp),
                ("g:price", None),   # se completa abajo (price / sale_price)
                ("g:google_product_category", categoria),
                ("g:product_type", product_type),
                ("g:condition", "new"),
                ("g:item_group_id", p["product_id"]) if len(variantes) > 1 else ("g:item_group_id", None),
                ("g:brand", marca),
                ("g:gender", gen),
                ("g:age_group", EDAD),
                ("g:color", color),
                *ids,
            ]
            i = next(n for n, (k, _) in enumerate(campos) if k == "g:price")
            if tachado and float(tachado) > float(precio_actual):
                campos[i:i + 1] = [("g:price", precio_fmt(tachado)),
                                   ("g:sale_price", precio_fmt(precio_actual))]
            else:
                campos[i] = ("g:price", precio_fmt(precio_actual))

            if publicar_google:
                items.append(item_xml(campos))

            # Feed "referencia": misma composición que el de Shaka (formato Uniwork).
            # Única diferencia: los relojes sí tienen GTIN/marca, así que se informan en vez de
            # declarar identifier_exists = no (Google lo marcaría como dato incorrecto).
            ref = [
                ("g:id", v["variant_id"]),
                ("g:title", titulo[:150]),
                ("g:description", desc[:5000]),
                ("g:link", link),
                ("g:image_link", imagen),
                ("g:checkout_link_template", link_variante),
                ("g:availability", disp),
                ("g:price", precio_fmt(precio_actual)),   # precio final que paga el cliente
                ("g:google_product_category", categoria),
                ("g:product_type", product_type),
                ("g:condition", "new"),
                ("g:item_group_id", p["product_id"]),
                *([("g:brand", marca)] if marca and (gtin or mpn) else []),
                *ids,
            ]
            items_ref.append(item_xml(ref))

    escribir_rss(items_ref, ruta)                                  # feed.xml = formato referencia
    escribir_rss(items, ruta.replace(".xml", "_google.xml"))      # feed_google.xml = completo para Google
    print(f"{ruta}: {len(items_ref)} items | {ruta.replace('.xml', '_google.xml')}: {len(items)} items "
          f"({omitidos} productos agotados omitidos)")


# ---------- 4. Escribir los feeds ----------

def main():
    try:
        with open("feed.json", encoding="utf-8") as f:
            previos = {p["url"]: p for p in json.load(f)["productos"]}
    except (OSError, ValueError, KeyError):
        previos = {}

    productos = catalogo_products_json()
    print(f"products.json: {len(productos)} productos | estados HTTP: {ESTADOS}", flush=True)
    if not productos:
        productos = catalogo_sitemap()
        print(f"Sitemap + .js (fallback): {len(productos)} productos", flush=True)

    # Protección: si la tienda falló y llegó un catálogo muy incompleto, no se pisa el feed bueno
    if previos and len(productos) < MIN_RATIO * len(previos):
        raise SystemExit(f"ERROR: solo {len(productos)} productos (antes {len(previos)}). "
                         "No se actualiza el feed en esta corrida.")

    completar_barcodes(productos, previos)

    generado = datetime.now(timezone.utc).isoformat()
    with open("feed.json", "w", encoding="utf-8") as f:
        json.dump({"generado": generado, "total": len(productos), "productos": productos},
                  f, ensure_ascii=False, indent=2)

    campos = ["product_id", "variant_id", "sku", "barcode", "marca", "nombre", "opciones", "precio",
              "precio_tachado", "disponible", "url", "imagen"]
    with open("feed.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        for p in productos:
            for v in p["variantes"]:
                w.writerow({
                    "product_id": p["product_id"], "marca": p["marca"], "nombre": p["nombre"],
                    "url": p["url"],
                    **{k: v.get(k) for k in ("variant_id", "sku", "barcode", "opciones", "precio",
                                             "precio_tachado", "disponible")},
                    "imagen": v.get("imagen") or (p["imagenes"][0] if p["imagenes"] else ""),
                })

    generar_google_xml(productos, "feed.xml")

    if time.time() - INICIO > TIEMPO_MAX:
        print("AVISO: se alcanzó el tiempo máximo; los códigos de barras faltantes se completan en la próxima corrida",
              flush=True)
    print(f"Estados HTTP: {ESTADOS}", flush=True)
    print(f"Listo: {len(productos)} productos en {time.time() - INICIO:.0f}s", flush=True)


if __name__ == "__main__":
    main()
