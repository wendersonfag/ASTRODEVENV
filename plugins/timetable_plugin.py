from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable, DagRunInfo, DataInterval
import pendulum

class BlackFridayTimetable(Timetable):
    """Timetable para executar a DAG com mais frequência na Black Friday."""

    def is_black_friday(self, current_date: pendulum.DateTime) -> bool:
        """Verifica se é a última sexta-feira de novembro (Black Friday)."""
        if current_date.month == 11 and current_date.weekday() == 4:
            last_day_of_november = current_date.end_of("month")
            return current_date.day > (last_day_of_november.day - 7)
        return False

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval, restriction) -> DagRunInfo | None:
        """Determina o próximo horário de execução da DAG."""
        next_start = last_automated_data_interval.end if last_automated_data_interval else pendulum.now()

        if self.is_black_friday(next_start):
            next_end = next_start.add(hours=1)  # Executa de hora em hora na Black Friday
        else:
            next_start = next_start.start_of("day").add(hours=9)  # Executa todo dia às 9h
            next_end = next_start.add(days=1)

        return DagRunInfo.exact(next_start)

# 🔹 Registrando a Timetable como Plugin para o Airflow reconhecê-la
class BlackFridayTimetablePlugin(AirflowPlugin):
    name = "black_friday_timetable_plugin"
    timetables = [BlackFridayTimetable]
