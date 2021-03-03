from urllib.parse import urlencode
from os import path
from pathlib import Path
import pandas as pd
import warnings


class Boliglan:
    COLUMNS = [
        "date",
        "product_id",
        "version_id",
        "leverandor_navn",
        "navn",
        "publiser_fra",
        "publiser_til",
        "leverandor_id",
        "leverandor_url",
        "markedsomraade",
        "rente_nominell",
        "rente_effektiv",
        "mnd_belop",
        "lanebelop",
        "kjopesum",
        "nedbetalingstid",
        "type_lan",
        "alder_min",
        "alder_max",
        "trenger_ikke_pakke",
        "produktpakkenavn",
        "forutsettermedlemskap",
        "belaningsgrad_max",
        "avdragsfrihet_max",
        "depotgebyr",
        "lopetid_max",
        "produktnavn",
        "termingebyr",
        "etableringsgebyr_for_lanet",
        "etableringsgebyr_type",
        "etableringsgebyr",
        "etableringsgebyr_prosent",
        "etableringsgebyr_min_belop",
        "etableringsgebyr_max_belop",
    ]

    COLUMN_RENAME = {
        "product id": "product_id",
        "version id": "version_id",
        "publiserFra": "publiser_fra",
        "publiserTil": "publiser_til",
        "leverandorId": "leverandor_id",
        "leverandorUrl": "leverandor_url",
        "leverandorVisningsnavn": "leverandor_navn",
        "nominellRente": "rente_nominell",
        "effectivRente": "rente_effektiv",
        "mndbelop": "mnd_belop",
        "Lanebelop": "lanebelop",
        "Kjopesum": "kjopesum",
        "Lopetidtermin Value": "nedbetalingstid",
        "Debetaing": "type_lan",
        "minAlder": "alder_min",
        "maksAlder": "alder_max",
        "produktpakkeNavn": "produktpakkenavn",
        "max_belaningsgrad": "belaningsgrad_max",
        "max_avdragsfrihet": "avdragsfrihet_max",
        "max_lopetid": "lopetid_max",
        "termingebyr_1_a": "termingebyr",
        "etableringsgebyrForLanet": "etableringsgebyr_for_lanet",
    }

    _CONFIG = {
        "boliglan": {
            "filepath": "./data/boliglan.csv",
            "urlpath": "https://www.finansportalen.no/services/kalkulator/boliglan/export",
            "query": {
                "kalkulatortype": "laan",
                "laan_type": "bolig",
                "rentetakIgnore": "ja",
                "lanebelop": 4000000,
                "kjopesum": 5000000,
                "lopetidtermin_value": 30,
                "alderstilbudAr": 30,
                "nasjonalt": "ja",
                "visProduktpakker": "ja",
                "visUtenProduktpakker": "ja",
                "jaforutsettermedlemskap": "ja",
                "neiforutsettermedlemskap": "ja",
                "rente": "flytende_rente",
                "standardlan": "ja",
                "rammelan": "ja",
                "forstehjemslan": "ja",
                "boliglan_for_unge": "ja",
                "gront_boliglan_miljoboliglan": "ja",
                "mellomfinansiering": "ja",
                "lan_fritidsbolig": "ja",
                "byggelan": "ja",
                "sortcolumn": "effectiveInterestRate%2CmonthlyPayment%2Cbank.name%2Cname",
                "sortdirection": "asc",
            },
        },
    }

    def __init__(self, loan_amount, home_value, repayment_period, person_age):
        Path("data/").mkdir(exist_ok=True)

        self.loan_amount = loan_amount
        self.home_value = home_value
        self.repayment_period = repayment_period
        self.person_age = person_age

        self.config = self._CONFIG["boliglan"]
        self.filepath = self.config["filepath"]
        self.urlpath = self.config["urlpath"]

    def _construct_url(self):
        config = self.config
        config["query"]["lanebelop"] = self.loan_amount
        config["query"]["kjopesum"] = self.home_value
        config["query"]["lopetidtermin_value"] = self.repayment_period
        config["query"]["alderstilbudAr"] = self.person_age

        params = urlencode(config["query"])
        url = f"{self.urlpath}?{params}"

        return url

    def _write_csv(self, df):
        try:
            df.to_csv(self.filepath, sep=";", encoding="utf-8", index=False)
        except Exception as e:
            print("_write_csv - Error:", e)

    def _load_csvfile(self):
        if path.exists(self.filepath) == True:
            df = pd.read_csv(self.filepath, sep=";", parse_dates=["date"])
        else:
            df = pd.DataFrame()

        return df

    def get_loans(self):
        warnings.simplefilter("ignore")
        url = self._construct_url()

        df = pd.read_excel(url, engine="openpyxl", date_parser="date")
        df["date"] = pd.to_datetime(df["date"], dayfirst=True)

        df = df.rename(columns=self.COLUMN_RENAME)
        df = df[self.COLUMNS]

        return df

    def update_data(self):
        df = self._load_csvfile()
        df_new = self.get_loans()

        merged = pd.merge(df_new, df, how="outer", indicator=True)
        new_data = merged.loc[merged._merge == "left_only", self.COLUMNS]

        if not new_data.empty:
            print("New data found:\n", new_data)

            df = pd.concat([df, new_data])
            self._write_csv(df)
