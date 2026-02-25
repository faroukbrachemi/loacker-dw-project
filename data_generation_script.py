import pandas as pd
import random
import math
from datetime import datetime, timedelta

# =========================
# REQUIREMENTS
# =========================
START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2025, 12, 31)

SHIFTS = ["A", "B"]  # 2 shifts
LINES = ["L1", "L2"]

PLANTS_CLEAN = [
    ("Plant Bolzano", "Bolzano", "Italy", "South Tyrol", "Europe"),
    ("Plant Vienna", "Vienna", "Austria", "East", "Europe"),
    ("Plant Chicago", "Chicago", "USA", "Midwest", "North America"),
    ("Plant Munich", "Munich", "Germany", "Bavaria", "Europe"),
]

PRODUCTS_CLEAN = [
    ("SKU001", "Loacker Wafer Vanilla", "Wafers", "Snacks", "Box"),
    ("SKU002", "Loacker Wafer Hazelnut", "Wafers", "Snacks", "Box"),
    ("SKU003", "Loacker Chocolate Classic", "Chocolate", "Sweets", "Wrapper"),
    ("SKU004", "Loacker Mini Mix", "Assortments", "Snacks", "Bag"),
    ("SKU005", "Loacker Coconut", "Wafers", "Snacks", "Box"),
    ("SKU006", "Loacker Dark Choco", "Chocolate", "Sweets", "Wrapper"),
    ("SKU007", "Loacker Protein Bar", "Bars", "Health", "Wrapper"),
    ("SKU008", "Loacker Family Pack", "Assortments", "Snacks", "Box"),
]

# =========================
# DIRTY DATA SETTINGS
# =========================
SEED = 42
random.seed(SEED)

P_MISSING_TEXT = 0.0
P_MISSING_NUM = 0.0
P_SPACES       = 0.12     # 12% add extra spaces around keys
P_CASE_NOISE   = 0.18     # 18% random case changes on some text values
P_BAD_NUM      = 0.01     # 1% negative/impossible numeric outliers
P_DUPLICATE    = 0.01     # 1% duplicate rows

# =========================
# HELPERS
# =========================
def maybe_missing_text(val: str) -> str:
    return "" if random.random() < P_MISSING_TEXT else val

def maybe_spaces(val: str) -> str:
    if val is None:
        return val
    if random.random() < P_SPACES:
        return f"  {val}   "
    return val

def maybe_case_noise(val: str) -> str:
    if val is None or val == "":
        return val
    if random.random() >= P_CASE_NOISE:
        return val
    mode = random.choice(["lower", "upper", "title", "weird"])
    if mode == "lower":
        return val.lower()
    if mode == "upper":
        return val.upper()
    if mode == "title":
        return val.title()
    # weird casing
    return "".join(ch.upper() if i % 2 == 0 else ch.lower() for i, ch in enumerate(val))

def maybe_missing_num(val) -> str:
    # return as string; sometimes empty
    if random.random() < P_MISSING_NUM:
        return ""
    return str(val)

def maybe_bad_num(val: float, kind: str) -> float:
    if random.random() >= P_BAD_NUM:
        return val
    # introduce some realistic "bad" values
    if kind in ("units_produced", "units_rejected"):
        return -abs(int(val))  # negative counts
    if kind == "quality":
        return 150.0           # impossible score
    if kind in ("emissions", "waste", "energy", "water"):
        return -abs(val)       # negative
    return val

def seasonality_wave(dt: datetime) -> float:
    x = 2 * math.pi * (int(dt.strftime("%j")) / 365.25)
    return 1.0 + 0.07 * math.sin(x)

PLANT_RULES = {
    "Plant Bolzano": {"emission_mult": 0.95, "waste_mult": 0.95, "quality_bonus": 0.5},
    "Plant Vienna": {"emission_mult": 1.00, "waste_mult": 1.00, "quality_bonus": 0.2},
    "Plant Chicago": {"emission_mult": 1.10, "waste_mult": 1.05, "quality_bonus": 0.0},
    "Plant Munich": {"emission_mult": 0.85, "waste_mult": 0.90, "quality_bonus": 1.0},  # Germany stricter
}

total_days = (END_DATE - START_DATE).days + 1
def time_factor(day_index: int) -> float:
    return day_index / max(1, total_days - 1)

# Sometimes mess up region names
REGION_NOISE = {
    "Bavaria": ["BAVARIA", "Bayern", "bavaria", "Bavaria "],
    "South Tyrol": ["SouthTyrol", "Südtirol", "south tyrol", "South Tyrol "],
    "Midwest": ["mid-west", "MIDWEST", "Mid West", "midwest"],
    "East": ["EAST", "Eastern", "east ", "East"],
}

# =========================
# GENERATE DIRTY ROWS
# =========================
rows = []
batch_counter = 1

current_date = START_DATE
day_index = 0

while current_date <= END_DATE:
    tf = time_factor(day_index)
    seas_mult = seasonality_wave(current_date)

    for plant in PLANTS_CLEAN:
        plant_name, city, country, region, continent = plant
        rules = PLANT_RULES[plant_name]

        for product in PRODUCTS_CLEAN:
            sku_code, product_name, product_line, category, packaging_type = product

            for shift in SHIFTS:
                batch_number = f"B-{current_date.strftime('%Y%m%d')}-{batch_counter:07d}"
                batch_counter += 1

                units_produced = random.randint(800, 2200)

                base_quality = random.uniform(72, 96)
                quality = min(100.0, base_quality + tf * 3.0 + rules["quality_bonus"])
                reject_rate = max(0.0, min(0.06, 0.06 - (quality - 70) / 1000))
                units_rejected = random.randint(0, int(units_produced * reject_rate))

                emissions = random.uniform(25, 115) * (1 - tf * 0.12) * rules["emission_mult"] * seas_mult * (units_produced / 1500.0)
                waste = random.uniform(1.2, 13.0) * (1 - tf * 0.10) * rules["waste_mult"] * (units_produced / 1500.0)
                energy = random.uniform(60, 340) * seas_mult * (units_produced / 1500.0)
                water = random.uniform(1200, 5600) * seas_mult * (units_produced / 1500.0)

                # inject bad numeric outliers occasionally
                units_produced = maybe_bad_num(units_produced, "units_produced")
                units_rejected = maybe_bad_num(units_rejected, "units_rejected")
                quality = maybe_bad_num(quality, "quality")
                emissions = maybe_bad_num(emissions, "emissions")
                waste = maybe_bad_num(waste, "waste")
                energy = maybe_bad_num(energy, "energy")
                water = maybe_bad_num(water, "water")

                # mess up region occasionally
                region_dirty = region
                if region in REGION_NOISE and random.random() < 0.20:
                    region_dirty = random.choice(REGION_NOISE[region])

                # apply text noise: missing/spaces/case
                row = {
                    "production_date": current_date.strftime("%Y-%m-%d"),

                    "batch_number": maybe_case_noise(maybe_spaces(maybe_missing_text(batch_number))),
                    "shift": maybe_case_noise(maybe_spaces(maybe_missing_text(shift))),
                    "line_number": maybe_case_noise(maybe_spaces(maybe_missing_text(random.choice(LINES)))),

                    "plant_name": maybe_case_noise(maybe_spaces(maybe_missing_text(plant_name))),
                    "city": maybe_case_noise(maybe_spaces(maybe_missing_text(city))),
                    "country": maybe_case_noise(maybe_spaces(maybe_missing_text(country))),
                    "region": maybe_case_noise(maybe_spaces(maybe_missing_text(region_dirty))),
                    "continent": maybe_case_noise(maybe_spaces(maybe_missing_text(continent))),

                    "sku_code": maybe_case_noise(maybe_spaces(maybe_missing_text(sku_code))),
                    "product_name": maybe_case_noise(maybe_spaces(maybe_missing_text(product_name))),
                    "product_line": maybe_case_noise(maybe_spaces(maybe_missing_text(product_line))),
                    "category": maybe_case_noise(maybe_spaces(maybe_missing_text(category))),
                    "packaging_type": maybe_case_noise(maybe_spaces(maybe_missing_text(packaging_type))),

                    # store numbers as strings (raw CSV reality), sometimes empty
                    "units_produced": maybe_missing_num(units_produced),
                    "units_rejected": maybe_missing_num(units_rejected),
                    "quality_test_score": maybe_missing_num(round(float(quality), 2) if quality != "" else ""),
                    "carbon_emissions_kg_co2e": maybe_missing_num(round(float(emissions), 3) if emissions != "" else ""),
                    "waste_kg": maybe_missing_num(round(float(waste), 3) if waste != "" else ""),
                    "energy_kwh": maybe_missing_num(round(float(energy), 3) if energy != "" else ""),
                    "water_liters": maybe_missing_num(round(float(water), 1) if water != "" else ""),
                }

                rows.append(row)

                # duplicate some rows (exact duplicates)
                if random.random() < P_DUPLICATE:
                    rows.append(dict(row))

    current_date += timedelta(days=1)
    day_index += 1

df = pd.DataFrame(rows)

out_file = "dirty_data_generated.csv"
df.to_csv(out_file, index=False)

print("Dirty CSV generated!")
print("Rows:", len(df))
print("File:", out_file)
print("Sample:")
print(df.head(3).to_string(index=False))
