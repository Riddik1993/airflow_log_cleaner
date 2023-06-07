import datetime
from typing import Iterable, Optional
import re
from minio import Minio
from minio.datatypes import Object


class AfLogsCleaner:
    """
    Сервис для очистки логов запуска дагов Airflow в Minio
    """
    regexp_extract_dagrun_dt = re.compile("run_id=\D+__(\d{4}-\d{2}-\d{2})")
    regexp_extract_dag_name = re.compile("dag_id=(\w+)/")

    def __init__(self, minio_url: str, access_key: str, secret_key: str, secure_connection_flg: bool,
                 dag_logs_bucket: str):
        """
        :param minio_url: url для подключения к Minio
        :param access_key: access_key для Minio
        :param secret_key: secret_key для Minio
        :param secure_connection_flg: флаг использования безопасного подключения к s3
        :param dag_logs_bucket: название бакета с логами дагов Airflow
        """
        self.minio_url = minio_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.dag_logs_bucket = dag_logs_bucket
        self.secure_connection_flg = secure_connection_flg
        self.client = Minio(endpoint=self.minio_url,
                            access_key=self.access_key,
                            secret_key=self.secret_key,
                            secure=self.secure_connection_flg)

    def list_log_files(self, dag_names: Optional[list[str]] = None) -> Iterable[Object]:
        """
        Метод составляет список имеющихся в Minio лог-файлов по указанным дагам

        :param dag_names: список с названиями дагов, по которым следует искать логи
        :return: итератор с лог-файлами (в виде объектов Minio)
        """
        objects = self.client.list_objects(self.dag_logs_bucket,
                                           recursive=True)
        if dag_names:
            dag_log_files_objects = (obj for obj in objects if
                                     self._extract_dag_name_from_path(obj.object_name) in dag_names)
        else:
            dag_log_files_objects = (obj for obj in objects if self._extract_dag_name_from_path(obj.object_name))

        return dag_log_files_objects

    def choose_expired_logs(self, log_files: Iterable[Object], logs_ttl_days: int) -> list[Object]:
        """
        Метод выбирает из переданного списка логов те, которые следует удалить

        :param log_files: список лог файлов дагов
        :param logs_ttl_days: число дней хранения логов
        :return: список лог файлов для удаления
        """
        now_dt = datetime.datetime.now().date()
        expire_deadline = now_dt - datetime.timedelta(days=logs_ttl_days)
        expired_lst = []

        for log_file in log_files:
            dag_run_dt = self._extract_dagrun_dt_from_path(log_file.object_name)
            if dag_run_dt < expire_deadline:
                expired_lst.append(log_file)
        return expired_lst

    def delete_expired_logs(self, log_files: list[Object]) -> None:
        """
        Метод удаляет из Minio указанные лог-файлы

        :param log_files: список лог файлов для удаления
        """
        if len(log_files) > 0:
            for file in log_files:
                self.client.remove_object(self.dag_logs_bucket, file.object_name)
                print(f"Deleted logfile: {file.object_name}")
        else:
            print(f"No logfiles to delete")

    def _extract_dagrun_dt_from_path(self, file_path: str) -> datetime.date:
        """
        Метод извлекает из полного пути лог-файла дату запуска дага

        :param file_path: полный путь лог файла в Minio
        :return: дата запуска дага
        """
        match_obj = self.regexp_extract_dagrun_dt.search(file_path)
        if match_obj:
            dag_run_dt_str = match_obj.group(1)
            dag_run_dt = datetime.datetime.strptime(dag_run_dt_str, "%Y-%m-%d").date()
            return dag_run_dt

    def _extract_dag_name_from_path(self, file_path: str) -> str:
        """
        Метод извлекает из полного пути лог-файла наименование дага

        :param file_path: полный путь лог файла в Minio
        :return: наименование дага
        """
        match_obj = self.regexp_extract_dag_name.search(file_path)
        if match_obj:
            dag_name = match_obj.group(1)
            return dag_name
