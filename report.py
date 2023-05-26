import json
import pandas as pd
import numpy as np


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class TrendsReport(object):
    index = 0

    def __init__(self) -> None:
        self.start_date = input("Start Date : ")
        self.end_date = input("End Date : ")
        self.filepath = input("Enter The CSV Filepath : ")
        # self.start_date = "2023-04-01"
        # self.end_date = "2023-04-09"
        # self.filepath = (
        #     "/Users/office/Desktop/TrendsReport Beta/Raw Athena CSV/12-18.csv"
        # )

        self.load_file = pd.read_csv(self.filepath)
        self.agent_data = self.load_file.iloc[:, [0, 1, 2, 3, 4]].copy()
        self.scores_data = self.load_file.iloc[
            :, [0, *[i for i in range(4, 44)]]
        ].copy()

    def get_call_id_per_agent_id(self, df: pd.DataFrame) -> dict:
        data = {}

        for row in df.iterrows():
            try:
                data[row[1]["agent_id"]].append(row[1]["call_id"])
            except KeyError:
                data[row[1]["agent_id"]] = [row[1]["call_id"]]

        return data

    def get_agent_name_per_agent_id(self, df: pd.DataFrame) -> dict:
        return dict(zip(df["agent_id"], df["agent_name"]))

    def get_supervisor_name_per_agent_id(self, df: pd.DataFrame) -> dict:
        return dict(zip(df["agent_id"], df["supervisor_name"]))

    def get_supervisor_id_per_agent_id(self, df: pd.DataFrame) -> dict:
        return dict(zip(df["agent_id"], df["supervisor_id"]))

    def get_skill_name_and_score_per_agent_id(self) -> dict:
        data = self.get_call_id_per_agent_id(self.agent_data)
        return {
            agent: self.select_skills(
                [(val, agent) for val in data[agent]], self.scores_data
            )
            for agent in data
        }

    def get_skill_list_by_agent(self) -> pd.DataFrame:
        skill_list_by_agent = self.get_skill_name_and_score_per_agent_id()
        agent_name = self.get_agent_name_per_agent_id(self.agent_data)
        supervisor_name = self.get_supervisor_name_per_agent_id(self.agent_data)
        supervisor_id = self.get_supervisor_id_per_agent_id(self.agent_data)
        return pd.DataFrame(
            [
                {
                    **skill,
                    "agent_id": agent,
                    "agent_name": agent_name[agent],
                    "supervisor_id": supervisor_id[agent],
                    "supervisor_name": supervisor_name[agent],
                }
                for agent in skill_list_by_agent
                for skill in skill_list_by_agent[agent]
            ]
        )

    def get_all_agents_per_skill(self, df: pd.DataFrame) -> dict:
        skill_list_by_agent = self.get_skill_list_by_agent()
        agent_by_skill = {
            skill: sorted(
                [
                    {
                        "agent_pk": row["agent_id"],
                        "agent_name": row["agent_name"],
                        "supervisor_id": row["supervisor_id"],
                        "supervisor_name": row["supervisor_name"],
                        "skill_score": row["skill_score"],
                        "call_count": row["call_count"],
                    }
                    for _, row in skill_list_by_agent[
                        skill_list_by_agent["skill_name"] == skill
                    ].iterrows()
                ],
                key=lambda d: d["skill_score"],
            )
            for skill in set(df.columns)
            & set(skill_list_by_agent["skill_name"].unique())
        }
        return agent_by_skill

    #! Change the val == 1 to val == "1" if query is from athena
    def get_yes(self, val: int) -> int:
        return 1 if val == 1 else 0

    def get_total_call(self, val: int):
        return 1 if val in [1, 2, 3] else 0

    def select_skills(self, call_list, df: pd.DataFrame):
        per_skill_calls = []
        selected_skills = []

        agent_df = df[df["agent_id"] == call_list[0][1]].drop_duplicates()
        TrendsReport.index += 1
        per_skill_calls += [dict(row[1]) for row in agent_df.iterrows()]

        new_df = pd.DataFrame(per_skill_calls)

        for col in df.columns:
            if col not in ["call_id", "agent_id"]:
                temp_val = (
                    sum(new_df[col].apply(self.get_yes))
                    / sum(new_df[col].apply(self.get_total_call))
                    if sum(new_df[col].apply(self.get_total_call)) > 0
                    else 0
                )
                temp_val = round(temp_val * 2, 1)

                try:
                    selected_skills.append(
                        {
                            "skill_name": col,
                            "skill_score": temp_val,
                            "call_count": sum(new_df[col].apply(self.get_total_call)),
                        }
                    )
                except KeyError:
                    selected_skills = [
                        {
                            "skill_name": col,
                            "skill_score": temp_val,
                            "call_count": sum(new_df[col].apply(self.get_total_call)),
                        }
                    ]

        return sorted(selected_skills, key=lambda d: d["skill_score"])

    def get_dataFrame(self) -> pd.DataFrame:
        """
        /*
         * Returns a pandas dataframe in a new format
         * obtained from loaded json file.
         */
        """
        json_data = self.get_all_agents_per_skill(self.scores_data)
        return pd.DataFrame.from_dict(
            [
                {
                    "agent_pk": item["agent_pk"],
                    "agent_name": item["agent_name"],
                    "supervisor_id": item["supervisor_id"],
                    "supervisor_name": item["supervisor_name"],
                    "skill_details": {
                        "skill_name": skill_name,
                        "skill_score": item["skill_score"],
                        "call_count": item["call_count"],
                    },
                }
                for skill_name, value in json_data.items()
                for item in value
            ]
        )

    def get_agent_report(self) -> list:
        df = self.get_dataFrame()
        return [
            {
                "agent_pk": int(id),
                "agent_name": df.loc[df["agent_pk"] == id, "agent_name"].iloc[0],
                "supervisor_id": df.loc[df["agent_pk"] == id, "supervisor_id"].iloc[0],
                "supervisor_name": df.loc[df["agent_pk"] == id, "supervisor_name"].iloc[
                    0
                ],
                "skill_details": sorted(
                    df.loc[df["agent_pk"] == id, "skill_details"].tolist(),
                    key=lambda d: d["skill_score"],
                ),
            }
            for id in df["agent_pk"].unique()
        ]

    def generate_result(self, data: list) -> None:
        with open(
            f"Reports/weekly_call_analyzer_result_"
            + f"from_({self.start_date})_to_({self.end_date})"
            + ".json",
            "+w",
        ) as file:
            json.dump(data, file, indent=2, cls=NpEncoder)

    def run(self) -> None:
        self.generate_result(self.get_agent_report())


trends = TrendsReport()
trends.run()
