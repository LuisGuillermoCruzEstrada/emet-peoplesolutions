from flask import Flask, request, jsonify, render_template, redirect, url_for
from logic.first100set import First100Set

app = Flask(
    __name__,
    template_folder="../templates",
    static_folder="../static"
)

# Estado en memoria (suficiente para prueba técnica)
number_set = First100Set()


@app.get("/")
def home():
    # missing_now solo tiene sentido después de extraer 1 número
    missing_now = number_set.find_missing() if len(number_set.numbers) < 100 else "-"
    return render_template(
        "index.html",
        error=None,
        result=None,
        remaining=len(number_set.numbers),
        missing_now=missing_now,
        last_value=None
    )


@app.post("/extract")
def extract_form():
    raw = request.form.get("number", "").strip()

    # Validación server-side (la importante)
    if raw == "":
        return render_template(
            "index.html",
            error="Debes ingresar un número.",
            result=None,
            remaining=len(number_set.numbers),
            missing_now=number_set.find_missing() if len(number_set.numbers) < 100 else "-",
            last_value=raw
        ), 400

    try:
        n = int(raw)
    except ValueError:
        return render_template(
            "index.html",
            error="El número debe ser un entero.",
            result=None,
            remaining=len(number_set.numbers),
            missing_now=number_set.find_missing() if len(number_set.numbers) < 100 else "-",
            last_value=raw
        ), 400

    try:
        number_set.extract(n)
        missing = number_set.find_missing()
    except ValueError as e:
        return render_template(
            "index.html",
            error=str(e),
            result=None,
            remaining=len(number_set.numbers),
            missing_now=number_set.find_missing() if len(number_set.numbers) < 100 else "-",
            last_value=n
        ), 400

    return render_template(
        "index.html",
        error=None,
        result={"extracted": n, "missing": missing},
        remaining=len(number_set.numbers),
        missing_now=missing,
        last_value=n
    )


@app.post("/reset")
def reset_set():
    global number_set
    number_set = First100Set()
    return redirect(url_for("home"))


# Si quieres mantener también el endpoint JSON (Postman), déjalo:
@app.post("/extract-json")
def extract_json():
    data = request.get_json(silent=True)

    if not data or "number" not in data:
        return jsonify({"error": "Debes enviar JSON con el campo 'number'"}), 400

    try:
        n = int(data["number"])
    except (ValueError, TypeError):
        return jsonify({"error": "El campo 'number' debe ser entero"}), 400

    try:
        number_set.extract(n)
        missing = number_set.find_missing()
        return jsonify({"message": f"Se extrajo el número {n}", "missing_number": missing})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400