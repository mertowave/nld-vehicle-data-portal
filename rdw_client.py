"""Shared tools for working with RDW vehicle data."""
from __future__ import annotations

from typing import Dict, Iterator, List, Optional

import os

import requests

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None  # type: ignore

BASE_URL = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
DEFAULT_PAGE_SIZE = 10000
EXCEL_MAX_ROWS = 1_048_576

COLUMN_TRANSLATIONS: Dict[str, str] = {
    "kenteken": "license_plate",
    "voertuigsoort": "vehicle_type",
    "merk": "make",
    "handelsbenaming": "commercial_name",
    "vervaldatum_apk": "inspection_expiry_date",
    "datum_tenaamstelling": "registration_date",
    "bruto_bpm": "gross_bpm",
    "inrichting": "body_style",
    "aantal_zitplaatsen": "seat_count",
    "eerste_kleur": "primary_color",
    "tweede_kleur": "secondary_color",
    "aantal_cilinders": "cylinder_count",
    "cilinderinhoud": "engine_displacement_cc",
    "massa_ledig_voertuig": "curb_weight_kg",
    "toegestane_maximum_massa_voertuig": "max_authorized_mass_kg",
    "massa_rijklaar": "ready_to_drive_mass_kg",
    "maximum_massa_trekken_ongeremd": "max_unbraked_tow_mass_kg",
    "maximum_trekken_massa_geremd": "max_braked_tow_mass_kg",
    "datum_eerste_toelating": "first_admission_date",
    "datum_eerste_tenaamstelling_in_nederland": "first_registration_nl_date",
    "wacht_op_keuren": "awaiting_inspection",
    "catalogusprijs": "list_price_eur",
    "wam_verzekerd": "liability_insured",
    "maximale_constructiesnelheid": "design_speed_kmh",
    "laadvermogen": "payload_kg",
    "oplegger_geremd": "semi_trailer_braked_mass_kg",
    "aanhangwagen_autonoom_geremd": "autonomous_trailer_braked_mass_kg",
    "aanhangwagen_middenas_geremd": "central_axle_trailer_braked_mass_kg",
    "aantal_staanplaatsen": "standing_places",
    "aantal_deuren": "door_count",
    "aantal_wielen": "wheel_count",
    "afstand_hart_koppeling_tot_achterzijde_voertuig": "distance_coupling_to_rear_mm",
    "afstand_voorzijde_voertuig_tot_hart_koppeling": "distance_front_to_coupling_mm",
    "afwijkende_maximum_snelheid": "alternate_max_speed_kmh",
    "lengte": "length_cm",
    "breedte": "width_cm",
    "europese_voertuigcategorie": "eu_vehicle_category",
    "europese_voertuigcategorie_toevoeging": "eu_vehicle_category_addition",
    "europese_uitvoeringcategorie_toevoeging": "eu_variant_category_addition",
    "plaats_chassisnummer": "vin_location",
    "technische_max_massa_voertuig": "technical_max_mass_kg",
    "type": "type_code",
    "type_gasinstallatie": "gas_installation_type",
    "typegoedkeuringsnummer": "type_approval_number",
    "variant": "variant",
    "uitvoering": "trim",
    "volgnummer_wijziging_eu_typegoedkeuring": "eu_type_approval_revision",
    "vermogen_massarijklaar": "power_mass_ratio_kw_per_kg",
    "wielbasis": "wheelbase_cm",
    "export_indicator": "export_indicator",
    "openstaande_terugroepactie_indicator": "open_recall_indicator",
    "vervaldatum_tachograaf": "tachograph_expiry_date",
    "taxi_indicator": "taxi_indicator",
    "maximum_massa_samenstelling": "max_combination_mass_kg",
    "aantal_rolstoelplaatsen": "wheelchair_places",
    "maximum_ondersteunende_snelheid": "max_assisted_speed_kmh",
    "jaar_laatste_registratie_tellerstand": "last_odometer_year",
    "tellerstandoordeel": "odometer_judgement",
    "code_toelichting_tellerstandoordeel": "odometer_judgement_code",
    "tenaamstellen_mogelijk": "registration_allowed",
    "vervaldatum_apk_dt": "inspection_expiry_datetime",
    "datum_tenaamstelling_dt": "registration_datetime",
    "datum_eerste_toelating_dt": "first_admission_datetime",
    "datum_eerste_tenaamstelling_in_nederland_dt": "first_registration_nl_datetime",
    "vervaldatum_tachograaf_dt": "tachograph_expiry_datetime",
    "maximum_last_onder_de_vooras_sen_tezamen_koppeling": "max_front_axle_load_with_coupling_kg",
    "type_remsysteem_voertuig_code": "brake_system_code",
    "rupsonderstelconfiguratiecode": "tracked_chassis_configuration_code",
    "wielbasis_voertuig_minimum": "wheelbase_min_cm",
    "wielbasis_voertuig_maximum": "wheelbase_max_cm",
    "lengte_voertuig_minimum": "length_min_cm",
    "lengte_voertuig_maximum": "length_max_cm",
    "breedte_voertuig_minimum": "width_min_cm",
    "breedte_voertuig_maximum": "width_max_cm",
    "hoogte_voertuig": "height_cm",
    "hoogte_voertuig_minimum": "height_min_cm",
    "hoogte_voertuig_maximum": "height_max_cm",
    "massa_bedrijfsklaar_minimaal": "operational_mass_min_kg",
    "massa_bedrijfsklaar_maximaal": "operational_mass_max_kg",
    "technisch_toelaatbaar_massa_koppelpunt": "tech_permissible_coupling_mass_kg",
    "maximum_massa_technisch_maximaal": "technical_mass_max_kg",
    "maximum_massa_technisch_minimaal": "technical_mass_min_kg",
    "subcategorie_nederland": "dutch_subcategory",
    "verticale_belasting_koppelpunt_getrokken_voertuig": "vertical_load_tow_point_kg",
    "zuinigheidsclassificatie": "efficiency_class",
    "registratie_datum_goedkeuring_afschrijvingsmoment_bpm": "bpm_depreciation_approval_date",
    "registratie_datum_goedkeuring_afschrijvingsmoment_bpm_dt": "bpm_depreciation_approval_datetime",
    "gem_lading_wrde": "avg_load_value",
    "aerodyn_voorz": "aerodynamic_features",
    "massa_alt_aandr": "alternative_drivetrain_mass_kg",
    "verl_cab_ind": "extended_cabin_indicator",
    "aantal_passagiers_zitplaatsen_wettelijk": "legal_passenger_seats",
    "aanwijzingsnummer": "designation_number",
    "api_gekentekende_voertuigen_assen": "api_axles_endpoint",
    "api_gekentekende_voertuigen_brandstof": "api_fuel_endpoint",
    "api_gekentekende_voertuigen_carrosserie": "api_bodywork_endpoint",
    "api_gekentekende_voertuigen_carrosserie_specifiek": "api_bodywork_specific_endpoint",
    "api_gekentekende_voertuigen_voertuigklasse": "api_vehicle_class_endpoint",
}

CSV_FIELDNAMES = sorted(COLUMN_TRANSLATIONS.values())

# Vehicle category translations (Dutch -> English)
CATEGORY_TRANSLATIONS: Dict[str, str] = {
    "Aanhangwagen": "Trailer",
    "Autonome aanhangwagen": "Autonomous trailer", 
    "Bedrijfsauto": "Commercial vehicle",
    "Bromfiets": "Moped",
    "Bus": "Bus",
    "Driewielig motorrijtuig": "Three-wheeled motor vehicle",
    "Land- of bosb aanhw of getr uitr stuk": "Agricultural or forestry trailer or towed equipment",
    "Land- of bosbouwtrekker": "Agricultural or forestry tractor",
    "Middenasaanhangwagen": "Central axle trailer",
    "Mobiele machine": "Mobile machine",
    "Motorfiets": "Motorcycle",
    "Motorfiets met zijspan": "Motorcycle with sidecar",
    "Motorrijtuig met beperkte snelheid": "Limited speed motor vehicle",
    "Oplegger": "Semi-trailer",
    "Personenauto": "Passenger car"
}


def translate_record(record: Dict[str, object]) -> Dict[str, object]:
    translated = {}
    for k, v in record.items():
        english_key = COLUMN_TRANSLATIONS.get(k, k)
        # Translate Dutch values to English
        english_value = translate_dutch_value(v)
        translated[english_key] = english_value
    return translated


def translate_dutch_value(value: object) -> object:
    """Translate Dutch values to English."""
    if value is None:
        return value
    
    # Convert to string for comparison
    str_value = str(value).strip()
    
    # Dutch to English value translations
    value_translations = {
        "Ja": "Yes",
        "Nee": "No",
        "ja": "Yes", 
        "nee": "No",
        "JA": "Yes",
        "NEE": "No",
        # Common Dutch phrases
        "Niet geregistreerd": "Not registered",
        "Niet van toepassing": "Not applicable",
        "Onbekend": "Unknown",
        "Leeg": "Empty",
        "Niet beschikbaar": "Not available",
        "Niet opgegeven": "Not specified",
        "Niet bekend": "Not known",
        "Niet ingevuld": "Not filled in",
        "Niet vermeld": "Not mentioned",
        "Niet opgenomen": "Not included",
        # Vehicle types
        "Aanhangwagen": "Trailer",
        "Autonome aanhangwagen": "Autonomous trailer",
        "Bedrijfsauto": "Commercial vehicle",
        "Bromfiets": "Moped",
        "Bus": "Bus",
        "Driewielig motorrijtuig": "Three-wheeled motor vehicle",
        "Land- of bosb aanhw of getr uitr stuk": "Agricultural or forestry trailer or towed equipment",
        "Land- of bosbouwtrekker": "Agricultural or forestry tractor",
        "Middenasaanhangwagen": "Central axle trailer",
        "Mobiele machine": "Mobile machine",
        "Motorfiets": "Motorcycle",
        "Motorfiets met zijspan": "Motorcycle with sidecar",
        "Motorrijtuig met beperkte snelheid": "Limited speed motor vehicle",
        "Oplegger": "Semi-trailer",
        "Personenauto": "Passenger car"
    }
    
    # Format dates if they look like dates (YYYYMMDD format)
    if len(str_value) == 8 and str_value.isdigit():
        try:
            year = str_value[:4]
            month = str_value[4:6]
            day = str_value[6:8]
            return f"{year}-{month}-{day}"
        except:
            pass
    
    return value_translations.get(str_value, value)


def build_filters(category: Optional[str], license_plate: Optional[str], brand: Optional[str] = None, model: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, order_by_recent: bool = False) -> Dict[str, str]:
    filters: Dict[str, str] = {}
    if category:
        filters["voertuigsoort"] = category
    if license_plate:
        normalized = license_plate.replace("-", "").replace(" ", "").upper()
        filters["kenteken"] = normalized
    if brand:
        # Brand filtering - case insensitive search
        filters["merk"] = brand.upper()
    if model:
        # Model filtering - case insensitive search
        filters["handelsbenaming"] = model.upper()
    
    # Handle date filtering with proper Socrata API format
    if date_from or date_to:
        date_conditions = []
        if date_from:
            # Convert YYYY-MM-DD to YYYYMMDD for API
            date_from_formatted = date_from.replace("-", "")
            date_conditions.append(f"datum_tenaamstelling >= '{date_from_formatted}'")
        if date_to:
            # Convert YYYY-MM-DD to YYYYMMDD for API
            date_to_formatted = date_to.replace("-", "")
            date_conditions.append(f"datum_tenaamstelling <= '{date_to_formatted}'")
        
        if date_conditions:
            filters["$where"] = " AND ".join(date_conditions)
    
    # Add ordering for recent records
    if order_by_recent:
        if "$where" in filters:
            filters["$where"] += " AND datum_tenaamstelling IS NOT NULL"
        else:
            filters["$where"] = "datum_tenaamstelling IS NOT NULL"
        filters["$order"] = "datum_tenaamstelling DESC"
    
    return filters


def fetch_rdw_data(
    *,
    limit: Optional[int],
    page_size: int,
    filters: Dict[str, str],
    app_token: Optional[str],
    timeout: float = 30.0,
) -> Iterator[Dict[str, object]]:
    fetched = 0
    offset = 0
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token

    session = requests.Session()

    while True:
        params: Dict[str, object] = {"$limit": page_size, "$offset": offset}
        params.update(filters)

        response = session.get(BASE_URL, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        rows: List[Dict[str, object]] = response.json()
        if not rows:
            break

        for row in rows:
            yield row
            fetched += 1
            if limit is not None and fetched >= limit:
                return

        if limit is not None and fetched >= limit:
            return

        if len(rows) < page_size:
            break
        offset += page_size


def resolve_app_token(cli_token: Optional[str] = None) -> Optional[str]:
    if cli_token:
        return cli_token
    return os.getenv("RDW_APP_TOKEN")


def export_to_excel(records: List[Dict[str, object]], path: str) -> None:
    if pd is None:
        raise RuntimeError("Install pandas and openpyxl to enable Excel export.")
    if len(records) > EXCEL_MAX_ROWS:
        raise RuntimeError(
            f"Excel cannot store {len(records):,} rows. Use CSV or filter the dataset."
        )
    df = pd.DataFrame.from_records(records)
    df.to_excel(path, index=False)


def translated_columns() -> List[str]:
    return [COLUMN_TRANSLATIONS[col] for col in sorted(COLUMN_TRANSLATIONS.keys())]


def fetch_categories(app_token: Optional[str], timeout: float = 30.0) -> List[str]:
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    params = {"$select": "distinct voertuigsoort", "$order": "voertuigsoort"}
    response = requests.get(BASE_URL, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    return [row["voertuigsoort"] for row in data if row.get("voertuigsoort")]


def translate_categories(categories: List[str]) -> List[str]:
    """Translate Dutch category names to English."""
    return [CATEGORY_TRANSLATIONS.get(cat, cat) for cat in categories]


def get_category_translation_map() -> Dict[str, str]:
    """Get mapping of Dutch to English category names."""
    return CATEGORY_TRANSLATIONS.copy()


def get_total_plate_count(app_token: str, timeout: float = 30.0) -> int:
    """Get total number of license plates in the database."""
    try:
        url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
        params = {
            "$select": "count(*)",
            "$$app_token": app_token
        }
        
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        if data and len(data) > 0:
            return int(data[0]["count"])
        return 0
        
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return 0


def get_available_brands(app_token: str, timeout: float = 30.0) -> List[str]:
    """Get list of available brands in the database."""
    try:
        url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
        params = {
            "$select": "merk",
            "$group": "merk",
            "$order": "merk",
            "$limit": 50000,  # Increased limit to get all brands
            "$$app_token": app_token
        }
        
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        brands = []
        for item in data:
            if item.get("merk") and item["merk"].strip():
                brands.append(item["merk"].strip())
        
        return sorted(brands)
        
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return []


def get_models_for_brand(brand: str, app_token: str, timeout: float = 30.0) -> List[str]:
    """Get list of models for a specific brand."""
    try:
        url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"
        params = {
            "$select": "handelsbenaming",
            "$where": f"merk = '{brand.upper()}'",
            "$group": "handelsbenaming",
            "$order": "handelsbenaming",
            "$limit": 1000,
            "$$app_token": app_token
        }
        
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        models = []
        for item in data:
            if item.get("handelsbenaming"):
                models.append(item["handelsbenaming"])
        
        return sorted(models)
        
    except (requests.RequestException, KeyError, ValueError, IndexError):
        return []


# Turkish translations for English column names (alphabetically sorted)
TURKISH_COLUMN_TRANSLATIONS: Dict[str, str] = {
    "alternate_max_speed_kmh": "Alternatif Maksimum Hız (km/h)",
    "autonomous_trailer_braked_mass_kg": "Otonom Römork Frenli Ağırlık (kg)",
    "awaiting_inspection": "Muayene Bekliyor",
    "body_style": "Karoseri",
    "central_axle_trailer_braked_mass_kg": "Merkezi Aks Römork Frenli Ağırlık (kg)",
    "commercial_name": "Ticari Ad",
    "curb_weight_kg": "Boş Ağırlık (kg)",
    "cylinder_count": "Silindir Sayısı",
    "design_speed_kmh": "Tasarım Hızı (km/h)",
    "distance_coupling_to_rear_mm": "Çeki Demiri-Arka Mesafe (mm)",
    "distance_front_to_coupling_mm": "Ön-Çeki Demiri Mesafe (mm)",
    "door_count": "Kapı Sayısı",
    "engine_displacement_cc": "Motor Hacmi (cc)",
    "eu_vehicle_category": "AB Araç Kategorisi",
    "eu_vehicle_category_addition": "AB Araç Kategorisi Eki",
    "eu_variant_category_addition": "AB Varyant Kategorisi Eki",
    "first_admission_date": "İlk Kabul Tarihi",
    "first_registration_nl_date": "Hollanda'da İlk Tescil Tarihi",
    "gas_installation_type": "Gaz Tesisatı Tipi",
    "gross_bpm": "Brüt BPM",
    "inspection_expiry_date": "Muayene Bitiş Tarihi",
    "length_cm": "Uzunluk (cm)",
    "liability_insured": "Sorumluluk Sigortalı",
    "license_plate": "Plaka",
    "list_price_eur": "Liste Fiyatı (EUR)",
    "make": "Marka",
    "max_authorized_mass_kg": "Maksimum Yetkili Ağırlık (kg)",
    "max_braked_tow_mass_kg": "Maksimum Frenli Çekme Ağırlığı (kg)",
    "max_unbraked_tow_mass_kg": "Maksimum Frenlenmemiş Çekme Ağırlığı (kg)",
    "payload_kg": "Yük Kapasitesi (kg)",
    "primary_color": "Ana Renk",
    "ready_to_drive_mass_kg": "Yolculuğa Hazır Ağırlık (kg)",
    "registration_date": "Tescil Tarihi",
    "secondary_color": "İkincil Renk",
    "seat_count": "Koltuk Sayısı",
    "semi_trailer_braked_mass_kg": "Yarı Römork Frenli Ağırlık (kg)",
    "standing_places": "Ayakta Yolcu Yeri",
    "technical_max_mass_kg": "Teknik Maksimum Ağırlık (kg)",
    "type_approval_number": "Tip Onay Numarası",
    "type_code": "Tip Kodu",
    "variant": "Varyant",
    "vehicle_type": "Araç Tipi",
    "vin_location": "Şasi Numarası Konumu",
    "wheel_count": "Tekerlek Sayısı",
    "width_cm": "Genişlik (cm)",
    "trim": "Donanım",
    "eu_type_approval_revision": "AB Tip Onay Revizyonu",
    "power_mass_ratio_kw_per_kg": "Güç-Ağırlık Oranı (kW/kg)",
    "wheelbase_cm": "Dingil Mesafesi (cm)",
    "export_indicator": "İhracat Göstergesi",
    "open_recall_indicator": "Açık Geri Çağırma Göstergesi",
    "tachograph_expiry_date": "Takoğraf Bitiş Tarihi",
    "taxi_indicator": "Taksi Göstergesi",
    "max_combination_mass_kg": "Maksimum Kombinasyon Ağırlığı (kg)",
    "wheelchair_places": "Tekerlekli Sandalye Yeri",
    "max_assisted_speed_kmh": "Maksimum Yardımlı Hız (km/h)",
    "last_odometer_year": "Son Kilometre Sayacı Yılı",
    "odometer_judgement": "Kilometre Sayacı Değerlendirmesi",
    "odometer_judgement_code": "Kilometre Sayacı Değerlendirme Kodu",
    "registration_allowed": "Tescil İzni",
    "inspection_expiry_datetime": "Muayene Bitiş Tarih Saati",
    "registration_datetime": "Tescil Tarih Saati",
    "first_admission_datetime": "İlk Kabul Tarih Saati",
    "first_registration_nl_datetime": "Hollanda'da İlk Tescil Tarih Saati",
    "tachograph_expiry_datetime": "Takoğraf Bitiş Tarih Saati",
    "max_front_axle_load_with_coupling_kg": "Maksimum Ön Aks Yükü Çeki Demiri ile (kg)",
    "brake_system_code": "Fren Sistemi Kodu",
    "tracked_chassis_configuration_code": "Paletli Şasi Konfigürasyon Kodu",
    "wheelbase_min_cm": "Minimum Dingil Mesafesi (cm)",
    "wheelbase_max_cm": "Maksimum Dingil Mesafesi (cm)",
    "length_min_cm": "Minimum Uzunluk (cm)",
    "length_max_cm": "Maksimum Uzunluk (cm)",
    "width_min_cm": "Minimum Genişlik (cm)",
    "width_max_cm": "Maksimum Genişlik (cm)",
    "height_cm": "Yükseklik (cm)",
    "height_min_cm": "Minimum Yükseklik (cm)",
    "height_max_cm": "Maksimum Yükseklik (cm)",
    "operational_mass_min_kg": "Minimum İşletme Ağırlığı (kg)",
    "operational_mass_max_kg": "Maksimum İşletme Ağırlığı (kg)",
    "tech_permissible_coupling_mass_kg": "Teknik İzin Verilen Çeki Demiri Ağırlığı (kg)",
    "technical_mass_max_kg": "Teknik Ağırlık Maksimum (kg)",
    "technical_mass_min_kg": "Teknik Ağırlık Minimum (kg)",
    "dutch_subcategory": "Hollanda Alt Kategorisi",
    "vertical_load_tow_point_kg": "Dikey Yük Çeki Noktası (kg)",
    "efficiency_class": "Verimlilik Sınıfı",
    "bpm_depreciation_approval_date": "BPM Amortisman Onay Tarihi",
    "bpm_depreciation_approval_datetime": "BPM Amortisman Onay Tarih Saati",
    "avg_load_value": "Ortalama Yük Değeri",
    "aerodynamic_features": "Aerodinamik Özellikler",
    "alternative_drivetrain_mass_kg": "Alternatif Güç Aktarım Ağırlığı (kg)",
    "extended_cabin_indicator": "Genişletilmiş Kabin Göstergesi",
    "legal_passenger_seats": "Yasal Yolcu Koltukları",
    "designation_number": "Atama Numarası",
    "api_axles_endpoint": "API Akslar Uç Noktası",
    "api_fuel_endpoint": "API Yakıt Uç Noktası",
    "api_bodywork_endpoint": "API Karoseri Uç Noktası",
    "api_bodywork_specific_endpoint": "API Karoseri Özel Uç Noktası",
    "api_vehicle_class_endpoint": "API Araç Sınıfı Uç Noktası"
}
