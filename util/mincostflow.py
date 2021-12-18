from ortools.graph import pywrapgraph

class MinCostFlow:
    def __init__(self):
        self.start_nodes = []
        self.end_nodes = []
        self.capacities = []
        self.unit_costs = []
        self.supplies = []
        self.free_host = []
        self.tasks = []
        self.min_cost_flow = pywrapgraph.SimpleMinCostFlow()

    def init(self):
        self.start_nodes = []
        self.end_nodes = []
        self.capacities = []
        self.unit_costs = []
        self.supplies = []
        self.free_host = []
        self.tasks = []
        self.min_cost_flow = pywrapgraph.SimpleMinCostFlow()

    def add_edge(self, start_node, end_node, capacity, unit_cost):
        self.start_nodes.append(start_node)
        self.end_nodes.append(end_node)
        self.capacities.append(capacity)
        self.unit_costs.append(unit_cost)

    def set_free_host(self, free_host):
        self.free_host = free_host

    def set_tasks(self, tasks):
        self.tasks = tasks

    def set_supply(self, supply):
        self.supplies.append(supply)

    def infer(self):
        for i in range(len(self.start_nodes)):
            self.min_cost_flow.AddArcWithCapacityAndUnitCost(
                self.start_nodes[i], self.end_nodes[i], self.capacities[i], self.unit_costs[i])
        for i in range(len(self.supplies)):
            self.min_cost_flow.SetNodeSupply(i, self.supplies[i])

    def find_next_task(self, host):
        if self.min_cost_flow.Solve() == self.min_cost_flow.OPTIMAL:
            if host not in self.free_host:
                print(host)
                print(self.free_host)
                return
            for i in range(self.min_cost_flow.NumArcs()):
                if self.min_cost_flow.Head(i) == self.free_host.index(host)+1+len(self.tasks) \
                        and self.min_cost_flow.Flow(i) > 0:
                    return self.tasks[self.min_cost_flow.Tail(i)-1]

    def print(self):
        print(self.start_nodes)
        print(self.end_nodes)
        print(self.capacities)
        print(self.unit_costs)
        print(self.supplies)
        if self.min_cost_flow.Solve() == self.min_cost_flow.OPTIMAL:
            print('Minimum cost:', self.min_cost_flow.OptimalCost())
            print('')
            print('  Arc    Flow / Capacity  Cost')
            for i in range(self.min_cost_flow.NumArcs()):
                cost = self.min_cost_flow.Flow(i) * self.min_cost_flow.UnitCost(i)
                print('%1s -> %1s   %3s  / %3s       %3s' % (
                    self.min_cost_flow.Tail(i),
                    self.min_cost_flow.Head(i),
                    self.min_cost_flow.Flow(i),
                    self.min_cost_flow.Capacity(i),
                    cost))
        else:
            print('There was an issue with the min cost flow input.')
