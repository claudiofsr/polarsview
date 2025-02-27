// https://docs.pola.rs/api/python/stable/reference/sql/clauses.html
// https://docs.pola.rs/api/python/stable/reference/sql/functions/index.html

const GROUP_BY_DATE: &str = "\
SELECT
    `Período de Apuração`,
    `Ano do Período de Apuração`,
    `Mês do Período de Apuração`,
    `Tipo de Operação`,
    `Tipo de Crédito`,
    SUM(`Valor da Base de Cálculo das Contribuições`) AS Base_de_Calculo,
    SUM(`Valor de PIS/PASEP`) AS Pis,
    SUM(`Valor de COFINS`) AS Cofins
FROM
    AllData
GROUP BY
    `Período de Apuração`,
    `Ano do Período de Apuração`,
    `Mês do Período de Apuração`,
    `Tipo de Operação`,
    `Tipo de Crédito`
ORDER BY
    `Período de Apuração`,
    CASE `Tipo de Operação`
        WHEN 'Saída' THEN 1
        WHEN 'Entrada' THEN 2
        WHEN 'Detalhamento' THEN 3
        WHEN 'Descontos' THEN 4
        ELSE 5  -- Lidar com outros valores inesperados
    END;\
";

// Predefined SQL commands for easy selection.
pub const SQL_COMMANDS: [&str; 12] = [
    "SELECT * FROM AllData;",
    "SELECT `Valor da Base de Cálculo das Contribuições` FROM AllData;",
    "SELECT * FROM AllData WHERE `Ano do Período de Apuração` = 2020;",
    "SELECT * FROM AllData WHERE `Mês do Período de Apuração` IS NULL;",
    "SELECT * FROM AllData WHERE `Data 1ª DCOMP Ativa` IS NOT NULL;",
    "SELECT * FROM AllData WHERE `Mês do Período de Apuração` = '';",
    "SELECT * FROM AllData WHERE `Ano do Período de Apuração` = 2020 AND `Mês do Período de Apuração` = 'março';",
    "SELECT * FROM AllData WHERE `Ano do Período de Apuração` = 2020 AND `Trimestre do Período de Apuração` = 3;",
    "SELECT `Tipo de Crédito`, COUNT(*) AS Frequencia FROM AllData GROUP BY `Tipo de Crédito` ORDER BY Frequencia DESC;",
    "SELECT `Tipo de Operação`, `Tipo de Crédito`, COUNT(*) FROM AllData GROUP BY `Tipo de Operação`, `Tipo de Crédito`;",
    "SELECT * FROM AllData WHERE `Natureza da Base de Cálculo dos Créditos` LIKE 'Saldo de Crédito%' AND `Mês do Período de Apuração` IS NULL;",
    GROUP_BY_DATE,
];
