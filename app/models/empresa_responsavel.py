from app import db


class EmpresaResponsavel(db.Model):
    __tablename__ = 'PAR002_EMPRESA_RESPONSAVEL_COBRANCA'
    __table_args__ = {'schema': 'dbo'}  # Especifica o esquema dbo para tabela externa

    pkEmpresaResponsavelCobranca = db.Column(db.Integer, primary_key=True)
    nmEmpresaResponsavelCobranca = db.Column(db.String(100), nullable=True)
    NO_ABREVIADO_EMPRESA = db.Column(db.String(20), nullable=True)

    def __repr__(self):
        return f'<EmpresaResponsavel {self.pkEmpresaResponsavelCobranca} - {self.nmEmpresaResponsavelCobranca}>'