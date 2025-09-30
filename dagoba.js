// ===== NAMESPACE =====
Dagoba = {}

// ===== GRAPH STRUCTURE =====
Dagoba.G = {}
Dagoba.graph = function(V, E) {
    var graph = Object.create(Dagoba.G)
    graph.edges = []
    graph.vertices = []
    graph.vertexIndex = {}
    graph.autoid = 1
    if(Array.isArray(V)) graph.addVertices(V)
    if(Array.isArray(E)) graph.addEdges(E)
    return graph
}

// ===== VERTEX OPERATIONS =====
Dagoba.G.addVertices = function(vs) { vs.forEach(this.addVertex.bind(this)) }
Dagoba.G.addVertex = function(vertex) {
    if(!vertex._id) vertex._id = this.autoid++
    else if(this.findVertexById(vertex._id))
        return Dagoba.error('ID exists')
    this.vertices.push(vertex)
    this.vertexIndex[vertex._id] = vertex
    vertex._out = []
    vertex._in = []
    return vertex._id
}

// ===== EDGE OPERATIONS =====
Dagoba.G.addEdges = function(es) { es.forEach(this.addEdge.bind(this)) }
Dagoba.G.addEdge = function(edge) {
    edge._in = this.findVertexById(edge._in)
    edge._out = this.findVertexById(edge._out)
    if(!(edge._in && edge._out)) return Dagoba.error("Missing vertex")
    edge._out._out.push(edge)
    edge._in._in.push(edge)
    this.edges.push(edge)
}

// ===== QUERY STRUCTURE =====
Dagoba.Q = {}
Dagoba.query = function(graph) {
    var q = Object.create(Dagoba.Q)
    q.graph = graph
    q.state = []
    q.program = []
    q.gremlins = []
    return q
}
Dagoba.Q.add = function(pt, args) {
    this.program.push([pt, args])
    return this
}
Dagoba.G.v = function() {
    return Dagoba.query(this).add('vertex', [].slice.call(arguments))
}

// ===== PIPETYPES =====
Dagoba.Pipetypes = {}
Dagoba.addPipetype = function(name, fn) {
    Dagoba.Pipetypes[name] = fn
    Dagoba.Q[name] = function() {
        return this.add(name, [].slice.apply(arguments))
    }
}
Dagoba.getPipetype = function(name) {
    var p = Dagoba.Pipetypes[name]
    if(!p) Dagoba.error('Unknown pipetype')
    return p || Dagoba.fauxPipetype
}
Dagoba.fauxPipetype = function() { return 'pull' }

// ===== CORE PIPETYPES =====
Dagoba.addPipetype('vertex', function(graph, args, gremlin, state) {
    if(!state.vertices) state.vertices = graph.findVertices(args)
    if(!state.vertices.length) return 'done'
    var v = state.vertices.pop()
    return Dagoba.makeGremlin(v, gremlin.state)
})
Dagoba.simpleTraversal = function(dir) {
    var fm = dir == 'out' ? 'findOutEdges' : 'findInEdges'
    var el = dir == 'out' ? '_in' : '_out'
    return function(graph, args, gremlin, state) {
        if(!gremlin && (!state.edges || !state.edges.length)) return 'pull'
        if(!state.edges || !state.edges.length) {
            state.gremlin = gremlin
            state.edges = graph[fm](gremlin.vertex).filter(Dagoba.filterEdges(args[0]))
        }
        if(!state.edges.length) return 'pull'
        var v = state.edges.pop()[el]
        return Dagoba.gotoVertex(state.gremlin, v)
    }
}
Dagoba.addPipetype('out', Dagoba.simpleTraversal('out'))
Dagoba.addPipetype('in', Dagoba.simpleTraversal('in'))
Dagoba.addPipetype('property', function(_, args, gremlin) {
    if(!gremlin) return 'pull'
    gremlin.result = gremlin.vertex[args[0]]
    return gremlin.result == null ? false : gremlin
})
Dagoba.addPipetype('filter', function(_, args, gremlin) {
    if(!gremlin) return 'pull'
    if(typeof args[0] == 'object')
        return Dagoba.objectFilter(gremlin.vertex, args[0]) ? gremlin : 'pull'
    if(typeof args[0] != 'function') return gremlin
    return args[0](gremlin.vertex, gremlin) ? gremlin : 'pull'
})
Dagoba.addPipetype('unique', function(_, args, gremlin, state) {
    if(!gremlin) return 'pull'
    if(state[gremlin.vertex._id]) return 'pull'
    state[gremlin.vertex._id] = true
    return gremlin
})
Dagoba.addPipetype('take', function(_, args, gremlin, state) {
    state.taken = state.taken || 0
    if(state.taken == args[0]) { state.taken = 0; return 'done' }
    if(!gremlin) return 'pull'
    state.taken++
    return gremlin
})
Dagoba.addPipetype('as', function(_, args, gremlin) {
    if(!gremlin) return 'pull'
    gremlin.state.as = gremlin.state.as || {}
    gremlin.state.as[args[0]] = gremlin.vertex
    return gremlin
})
Dagoba.addPipetype('merge', function(_, args, gremlin, state) {
    if(!state.vertices && !gremlin) return 'pull'
    if(!state.vertices || !state.vertices.length) {
        var obj = (gremlin.state || {}).as || {}
        state.vertices = args.map(id => obj[id]).filter(Boolean)
    }
    if(!state.vertices.length) return 'pull'
    var v = state.vertices.pop()
    return Dagoba.makeGremlin(v, gremlin.state)
})
Dagoba.addPipetype('except', function(_, args, gremlin) {
    if(!gremlin) return 'pull'
    return gremlin.vertex == gremlin.state.as[args[0]] ? 'pull' : gremlin
})
Dagoba.addPipetype('back', function(_, args, gremlin) {
    if(!gremlin) return 'pull'
    return Dagoba.gotoVertex(gremlin, gremlin.state.as[args[0]])
})

// ===== HELPERS =====
Dagoba.error = function(msg) { console.log(msg); return false }
Dagoba.makeGremlin = function(v, s) { return { vertex: v, state: s || {} } }
Dagoba.gotoVertex = function(g, v) { return Dagoba.makeGremlin(v, g.state) }
Dagoba.G.findVertices = function(args) {
    if(typeof args[0] == 'object') return this.searchVertices(args[0])
    if(!args.length) return this.vertices.slice()
    return this.findVerticesByIds(args)
}
Dagoba.G.findVerticesByIds = function(ids) {
    if(ids.length == 1) {
        var v = this.findVertexById(ids[0])
        return v ? [v] : []
    }
    return ids.map(this.findVertexById.bind(this)).filter(Boolean)
}
Dagoba.G.findVertexById = function(id) { return this.vertexIndex[id] }
Dagoba.G.searchVertices = function(f) {
    return this.vertices.filter(v => Dagoba.objectFilter(v, f))
}
Dagoba.G.findInEdges = v => v._in
Dagoba.G.findOutEdges = v => v._out
Dagoba.filterEdges = filter => edge => {
    if(!filter) return true
    if(typeof filter == 'string') return edge._label == filter
    if(Array.isArray(filter)) return filter.includes(edge._label)
    return Dagoba.objectFilter(edge, filter)
}
Dagoba.objectFilter = function(obj, f) {
    for(var k in f) if(obj[k] !== f[k]) return false
    return true
}

// ===== INTERPRETER =====
Dagoba.Q.run = function() {
    this.program = Dagoba.transform(this.program)
    var max = this.program.length - 1, maybe = false, res = [], done = -1, pc = max
    while(done < max) {
        var step = this.program[pc]
        var state = this.state[pc] = this.state[pc] || {}
        var fn = Dagoba.getPipetype(step[0])
        maybe = fn(this.graph, step[1], maybe, state)
        if(maybe == 'pull') {
            maybe = false
            if(pc-1 > done) { pc--; continue }
            else done = pc
        }
        if(maybe == 'done') { maybe = false; done = pc }
        pc++
        if(pc > max) {
            if(maybe) res.push(maybe)
            maybe = false
            pc--
        }
    }
    return res.map(g => g.result != null ? g.result : g.vertex)
}

// ===== TRANSFORMERS =====
Dagoba.T = []
Dagoba.addTransformer = function(f, p) {
    for(var i = 0; i < Dagoba.T.length; i++)
        if(p > Dagoba.T[i].priority) break
    Dagoba.T.splice(i, 0, { priority: p, fun: f })
}
Dagoba.transform = function(prog) {
    return Dagoba.T.reduce((acc, t) => t.fun(acc), prog)
}

// ===== ALIASES =====
Dagoba.addAlias = function(n, o, def) {
    def = def || []
    Dagoba.addTransformer(function(prog) {
        return prog.map(step => step[0] != n ? step : [o, Dagoba.extend(step[1], def)])
    }, 100)
    Dagoba.addPipetype(n, () => {})
}
Dagoba.extend = function(list, defaults) {
    return Object.keys(defaults).reduce((acc, k) => {
        if(list[k] !== undefined) return acc
        acc[k] = defaults[k]
        return acc
    }, list)
}

// ===== SERIALIZATION =====
Dagoba.jsonify = function(g) {
    return '{"V":' + JSON.stringify(g.vertices, Dagoba.cleanVertex) +
           ',"E":' + JSON.stringify(g.edges, Dagoba.cleanEdge) + '}'
}
Dagoba.cleanVertex = function(k, v) {
    return (k == '_in' || k == '_out') ? undefined : v
}
Dagoba.cleanEdge = function(k, v) {
    return (k == '_in' || k == '_out') ? v._id : v
}
Dagoba.G.toString = function() { return Dagoba.jsonify(this) }
Dagoba.fromString = function(str) {
    var obj = JSON.parse(str)
    return Dagoba.graph(obj.V, obj.E)
}

// ===== PERSISTENCE =====
Dagoba.persist = function(g, name) {
    name = name || 'graph'
    localStorage.setItem('DAGOBA::' + name, g)
}
Dagoba.depersist = function(name) {
    name = 'DAGOBA::' + (name || 'graph')
    return Dagoba.fromString(localStorage.getItem(name))
}

// ===== EXAMPLE ALIASES =====
Dagoba.addAlias('parents', 'out', ['parent'])
Dagoba.addAlias('children', 'in', ['parent'])
