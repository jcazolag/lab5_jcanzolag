from mrjob.job import MRJob

class MRPuntoUno_Uno(MRJob):

    def mapper(self, _, line):
        idemp, sector,salary,year = line.split(',')
        yield sector, int(salary)

    def reducer(self, sector, values):
        l = list(values)
        avg = sum(l) / len(l)
        yield sector, avg


class MRPuntoUno_Dos(MRJob):
    
    def mapper(self, _, line):
        idemp, sector,salary,year = line.split(',')
        yield idemp, int(salary)

    def reducer(self, idemp, values):
        l = list(values)
        avg = sum(l) / len(l)
        yield idemp, avg

class MRPuntoUno_Tres(MRJob):
    
    def mapper(self, _, line):
        idemp, sector,salary,year = line.split(',')
        yield idemp,sector


    def reducer(self, idemp, values):
        l = list(values)
        yield idemp, len(l)
    

if __name__ == '__main__':
    print("---------------------------------------------------------------------------------------------------------------------------")
    print("\t\t\tPUNTO 1-1: El salario promedio por Sector Económico (SE)")
    print("---------------------------------------------------------------------------------------------------------------------------")
    MRPuntoUno_Uno.run()
    print("---------------------------------------------------------------------------------------------------------------------------")
    print("\t\t\tPUNTO 1-2: El salario promedio por Empleado")
    print("---------------------------------------------------------------------------------------------------------------------------")
    MRPuntoUno_Dos.run()
    print("---------------------------------------------------------------------------------------------------------------------------")
    print("\t\t\tPUNTO 1-3: Número de SE por Empleado que ha tenido a lo largo de la estadística")
    print("---------------------------------------------------------------------------------------------------------------------------")
    MRPuntoUno_Tres.run()
    