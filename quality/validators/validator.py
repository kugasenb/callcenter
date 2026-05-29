class Validator:

    @staticmethod
    def run(df, rules):

        resultados = []

        for rule in rules:

            resultado = rule(df)

            resultados.append(resultado)

        return resultados