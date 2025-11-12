import pandas as pd

df = pd.read_csv("covid_data.csv")

# 1. Global infections
global_infected = df["TotalCases"].sum()
print("Global infections:", global_infected)

# 2. Cases by continent
continent_cases = df.groupby("Continent")["TotalCases"].sum()
print("\nCases by continent:\n", continent_cases)

# 3. Country with maximum deaths
max_death_country = df.loc[df["TotalDeaths"].idxmax(), ["Country", "TotalDeaths"]]
print("\nCountry with maximum deaths:\n", max_death_country)

# 4. Total people vaccinated globally
total_vaccinated = df["TotalVaccinations"].sum()
print("\nTotal vaccinated globally:", total_vaccinated)
