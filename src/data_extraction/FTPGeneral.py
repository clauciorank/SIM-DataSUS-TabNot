from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pandas as pd


class DownloadFileGeneral:
    def __init__(self, states: list, start_year: int, end_year: int, system, file_path: str):
        self.states = states
        self.start_year = start_year
        self.end_year = end_year
        self.system = system
        self.file_path = file_path

    def list_all_files(self):
        if self.system.name == 'SIM':
            files = self.system.get_files('CID10', uf=self.states, year=self.list_with_years())
            return files
        else:
            raise ValueError('System is not Valid')

    def list_with_years(self):
        return list(range(int(self.start_year), int(self.end_year) + 1))

    def describe_files(self):
        files = self.list_all_files()
        if files is None or len(files) == 0:
            raise ValueError('Files not found in FTP')

        descriptions = [self.system.describe(x) for x in files]
        return pd.DataFrame(descriptions)

    def list_local_files(self) -> pd.DataFrame:
        """Retorna metadados dos arquivos já baixados no diretório local."""
        path = Path(self.file_path)
        if not path.exists():
            return pd.DataFrame(columns=['name', 'path', 'size', 'modified'])

        metadados = []
        for f in path.iterdir():
            # pysus pode salvar parquet como diretório (particionado) ou como arquivo
            if f.suffix.lower() not in ('.parquet', '.dbf', '.dbc'):
                continue
            if f.is_file():
                stat = f.stat()
                metadados.append({
                    'name': f.name,
                    'path': str(f.absolute()),
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime),
                })
            elif f.is_dir() and f.name.endswith('.parquet'):
                # diretório parquet particionado
                total_size = sum(p.stat().st_size for p in f.rglob('*') if p.is_file())
                mtimes = [p.stat().st_mtime for p in f.rglob('*') if p.is_file()]
                modified = datetime.fromtimestamp(max(mtimes)) if mtimes else datetime.fromtimestamp(f.stat().st_mtime)
                metadados.append({
                    'name': f.name,
                    'path': str(f.absolute()),
                    'size': total_size,
                    'modified': modified,
                })
        return pd.DataFrame(metadados, columns=['name', 'path', 'size', 'modified'])

    def verify_if_need_download(self):
        """
        Verifica se precisa realizar download de algum arquivo.
        Retorna lista de arquivos FTP que precisam ser baixados (não existem
        localmente ou têm data de atualização mais recente no FTP).
        """
        ftp_files = self.list_all_files()
        if not ftp_files:
            return []

        path = Path(self.file_path)
        path.mkdir(parents=True, exist_ok=True)

        # Mapeia arquivos locais por nome base (sem extensão) para comparação
        local_by_basename = {}
        if path.exists():
            for f in path.iterdir():
                if f.suffix.lower() not in ('.parquet', '.dbf', '.dbc'):
                    continue
                if f.is_file():
                    local_by_basename[f.stem] = f.stat().st_mtime
                elif f.is_dir() and f.name.endswith('.parquet'):
                    mtimes = [p.stat().st_mtime for p in f.rglob('*') if p.is_file()]
                    local_by_basename[f.stem] = max(mtimes) if mtimes else f.stat().st_mtime

        files_to_download = []
        for ftp_file in ftp_files:
            desc = self.system.describe(ftp_file)
            ftp_modify_str = desc.get('last_update', '')
            local_mtime = local_by_basename.get(ftp_file.name)

            if local_mtime is None:
                files_to_download.append(ftp_file)
                continue

            try:
                ftp_datetime = datetime.strptime(ftp_modify_str, '%Y-%m-%d %I:%M%p')
            except ValueError:
                ftp_datetime = datetime.strptime(ftp_modify_str, '%Y-%m-%d %H:%M')
            ftp_timestamp = ftp_datetime.timestamp()

            if ftp_timestamp > local_mtime:
                files_to_download.append(ftp_file)

        return files_to_download

    def download_files(
        self,
        force_all: bool = False,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ):
        """
        Faz download dos arquivos no diretório padrão (self.file_path).
        Por padrão, baixa apenas os que precisam de atualização.
        Se force_all=True, baixa todos os arquivos da lista.
        progress_callback: opcional, chamado (atual, total, nome_arquivo) após cada arquivo.
        Retorna lista de caminhos dos arquivos baixados.
        """
        Path(self.file_path).mkdir(parents=True, exist_ok=True)

        if force_all:
            files_to_download = self.list_all_files()
        else:
            files_to_download = self.verify_if_need_download()

        if not files_to_download:
            return []

        total = len(files_to_download)
        results = []

        for i, ftp_file in enumerate(files_to_download):
            if progress_callback:
                progress_callback(i, total, ftp_file.basename)
            data = ftp_file.download(local_dir=self.file_path)
            results.append(data)
            if progress_callback:
                progress_callback(i + 1, total, ftp_file.basename)

        return [str(r.path) if hasattr(r, 'path') else str(r) for r in results]




if __name__ == '__main__':
    from pysus import SIM
    sim = SIM().load()


    ftp = DownloadFileGeneral(['AC', 'PR'], start_year=2022, end_year=2024, system=sim, file_path='/home/claucio/Documents/DatasusBrasileiroApp/data/SIM/raw')
    ftp.list_local_files()
    ftp.describe_files()

