from gurobipy import *



#input data 
R = ['Carlos','Joe','Monika']
J = ['Tester','JavaDeveloper','Architect']

#matching score data
combinations, ms = multidict({
	('Carlos','Tester'):53,
	('Carlos','JavaDeveloper'):27,
	('Carlos','Architect'):13,

	('Joe','Tester'):80,
	('Joe','JavaDeveloper'):47,
	('Joe','Architect'):67,

	('Monika','Tester'):53,
	('Monika','JavaDeveloper'):73,
	('Monika','Architect'):47

	})

#declare and init model
m = Model('RAP')

#decision variables
x = m.addVars(combinations, name = "assign")

#constraints
jobs = m.addConstrs(((x.sum('*',j)) == 1 for j in J),'job')
resources = m.addConstrs(((x.sum(r,'*')) <= 1 for r in R),'resource')

#objective functn
m.setObjective(x.prod(ms), GRB.MAXIMIZE)

#save model
m.write('RAP.lp')

m.optimize()

for v in m.getVars():
	print(v.varname, v.x)
print(m.objVal)