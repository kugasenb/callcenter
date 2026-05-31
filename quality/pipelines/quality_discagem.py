from quality.rules.rules import Rules


PIPELINE_CONFIG = {

    "nm_pipeline": "quality_discagem"
    , "path": "/Volumes/workspace/callcenter/disc_volumes/landing/"
    , "rules": [
        Rules.validar_telefone_nulo
        , Rules.validar_data_nula
        , Rules.validar_padrao_data
    ]
}