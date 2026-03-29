import pandas as pd
data = [
    ["3512", "Raoul", "USA", "Journalist"],
    ["3699", "Gonzo", "USA", "Doctor"],
    ["4242", "Slartibartfast", "MAG", "Architect"],
    ["3187", "Arthur", "GBR", "Writer"]
]

columns = ["ID", "FirstName", "Country", "Role"]
df = pd.DataFrame(data, columns=columns)
file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees.csv"
df.to_csv(file_path, index=False)