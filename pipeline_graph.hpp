//********************************************************************************
// Copyright (c) 2014-2015 Yves Vandriessche. All rights reserved.              **
//                                                                              **
// Redistribution and use in source and binary forms, with or without           **
// modification, are permitted provided that the following conditions are met:  **
//   * Redistributions of source code must retain the above copyright notice,   **
//     this list of conditions and the following disclaimer.                    **
//   * Redistributions in binary form must reproduce the above copyright        **
//     notice, this list of conditions and the following disclaimer in the      **
//     documentation and/or other materials provided with the distribution.     **
//   * Neither the name of Intel Corporation nor the names of its contributors  **
//     may be used to endorse or promote products derived from this software    **
//     without specific prior written permission.                               **
//                                                                              **
// This software is provided by the copyright holders and contributors "as is"  **
// and any express or implied warranties, including, but not limited to, the    **
// implied warranties of merchantability and fitness for a particular purpose   **
// are disclaimed. In no event shall the copyright owner or contributors be     **
// liable for any direct, indirect, incidental, special, exemplary, or          **
// consequential damages (including, but not limited to, procurement of         **
// substitute goods or services; loss of use, data, or profits; or business     **
// interruption) however caused and on any theory of liability, whether in      **
// contract, strict liability, or tort (including negligence or otherwise)      **
// arising in any way out of the use of this software, even if advised of       **
// the possibility of such damage.                                              **
//********************************************************************************

#ifndef PIPELINE_GRAPH_H
#define PIPELINE_GRAPH_H

#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
//#include <boost/graph/graphviz.hpp>
//#include <boost/graph/subgraph.hpp>
#include <boost/property_map/property_map.hpp>
#include <boost/graph/adj_list_serialize.hpp>
#include <boost/serialization/string.hpp>
//#include <string>
#include <boost/serialization/unordered_map.hpp>
//#include <unordered_map>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/range/iterator_range.hpp>
//#include <unordered_map>
#include <map>
#include <unordered_set>
#include <functional>

using boost::get;
using boost::vertex_index;
using boost::vertices;
using boost::edges;
using boost::make_iterator_range;

using std::vector;
using std::string;



enum collection_t { STAGE_COL, DATA_COL, COUNT };
enum dependency_t { PRODUCES, CONSUMES, FANIN, FANOUT };
namespace boost {
  enum edge_dependency_t { edge_dependency };
  BOOST_INSTALL_PROPERTY(edge, dependency);
}
typedef int subgraph_idx_t;
static const subgraph_idx_t THE_ROOT_GRAPH=0;
struct PLNode {
  collection_t type;
  string name;
  string command;
  subgraph_idx_t subgraph;
  //vector<string> input_ports, output_ports;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version) {
    ar & type
      & name
      & command
      & subgraph;
  }
};

struct PLEdge {
  dependency_t type;
  string name;
  int index;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version) {
	ar & type;
	ar & name;
	ar & index;
  }
};

class PLGraph {
private:
  // boost graph types //////////////////////////////////////////////////////////
  // typedef property<vertex_color_t, collection_t, 
  // 		   property<vertex_name_t, string>> VertexProperty;
  // typedef property<edge_index_t, int,
  // 		   property<edge_dependency_t, dependency_t>> EdgeProperty;
  typedef boost::adjacency_list< boost::vecS, boost::vecS, boost::bidirectionalS, PLNode, PLEdge > BoostGraph;
  typedef typename boost::graph_traits<BoostGraph>::vertex_descriptor vertex_t;
  typedef typename boost::graph_traits<BoostGraph>::edge_descriptor edge_t;
  
  // a label -> vertex map for easy by name node lookup
  typedef std::unordered_map<string, vertex_t> NodesMap;
  ////////////////////////////////////////////////////////////////////////////////
  NodesMap    dependency_map;
  BoostGraph  m_graph;
  // this subgraph pointer object to the 'current graph' under subgraph nesting construction
  subgraph_idx_t m_subgraph_label=THE_ROOT_GRAPH;

public:

  // PLGraph( PLGraph&& other ): dependency_map(std::move(other.dependency_map)),
  //   			      m_graph(std::move(other.m_graph)),
  //   			      m_subgraph(&m_graph) {}
  // PLGraph( PLGraph& other ): dependency_map(other.dependency_map),
  //  			     m_graph(m_graph),
  //  			     m_subgraph(&m_graph) {}

  
  //PLGraph(): m_subgraph(&m_graph) {};
  // PLGraph( PLGraph& )=delete;
  // PLGraph( PLGraph&& )=default;
  
  // run given code (lambda) in a 'context' where this object has a new subgraph nesting
  //   this member function will take care of the subgraph data member modification
  // although the subgraph member is not accessible from outside, other PLGraph members are contextually sensitive to it
  void inside_subgraph_context(std::function<void()> fn) {
    static subgraph_idx_t new_subgraph_label=THE_ROOT_GRAPH;
           subgraph_idx_t parent_label=m_subgraph_label;
    m_subgraph_label = ++new_subgraph_label;
    fn();
    m_subgraph_label = parent_label;
  }

  // use this to access and construct (data-collection!) vertices on demand
  vertex_t dependency_vertex(const string label) { 
    auto entry = dependency_map.find(label);
    if ( entry == dependency_map.end() ) {
      auto vertex = add_vertex(m_graph);
      m_graph[vertex] = { DATA_COL, label, "", m_subgraph_label };
      dependency_map.insert( std::make_pair(label, vertex) );
      return vertex;
    }
    else
      return entry->second;
  }

  vertex_t stage_vertex(const string& label, const string& command) {
    // we do not remember the stage nodes, under the assumption that each given stage name node is unique
    auto vertex = add_vertex(m_graph);
    // m_graph[vertex].type     = STAGE_COL;
    // m_graph[vertex].name     = label;
    // m_graph[vertex].command  = command;
    // m_graph[vertex].subgraph = m_subgraph_label;
    m_graph[vertex] = { STAGE_COL, label, command, m_subgraph_label };
    return vertex;
  }

  vertex_t count_vertex(const string& command) {
    auto count_v = add_vertex(m_graph);
    // m_graph[count_v].type     = COUNT;
    // m_graph[count_v].name     = "count";
    // m_graph[count_v].command  = command;
    // m_graph[count_v].subgraph = m_subgraph_label;
    m_graph[count_v] = { COUNT, "count", command, m_subgraph_label };
    return count_v;
  }
  
  edge_t wire(vertex_t v1, vertex_t v2, dependency_t type, string label="") {
    edge_t edge = add_edge( v1, v2, m_graph ).first;
    // put(edge_dependency, m_graph, edge, type);
    auto& edge_descriptor = m_graph[edge];
    edge_descriptor.type = type;
    edge_descriptor.name = label;
    //    std::cout << " wiring " << v1 << " to " << v2 << " with label " << edge_descriptor.name << std::endl;
    return edge;
  }
  edge_t wire_produce(vertex_t v1, vertex_t v2, string label="") { return wire( v1, v2, PRODUCES, label ); }
  edge_t wire_consume(vertex_t v1, vertex_t v2, string label="") { return wire( v1, v2, CONSUMES, label ); }
  edge_t   wire_fanin(vertex_t v1, vertex_t v2) { return wire( v1, v2, FANIN    ); }
  edge_t  wire_fanout(vertex_t v1, vertex_t v2) { return wire( v1, v2, FANOUT   ); }

  // boost serialization functionality
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & dependency_map;
    ar & m_graph;
  }

  // bool is_split_stage( vertex_t v ) {
  //   const auto outgoing_range = out_edges(v, m_graph);
  //   std::any_of( outgoing_range.first, outgoing_range.second,
  // 		 [this](const edge_t& e){
  // 		   return m_graph[e].type == FANOUT;
  // 		 });
  // }
  // bool is_join_stage( vertex_t v ) {
  //   const auto incoming_range = in_edges(v, m_graph);
  //   std::any_of( incoming_range.first, incoming_range.second,
  // 		 [this](const edge_t& e){
  // 		   return m_graph[e].type == FANIN;
  // 		 });
  // }
  
  const string& source_name( const edge_t e ) {
    const auto& v = source(e,m_graph);
    return m_graph[v].name;					   
  }
  const string& target_name( const edge_t e ) {
    const auto& v = target(e,m_graph);
    return m_graph[v].name;					   
  }

  typedef vector<string> names_t;

  names_t consumes_names(const vertex_t& v, const BoostGraph& g) {
    names_t names;
    for ( const edge_t& e : make_iterator_range( in_edges(v,g) ) )
      if ( g[e].type == CONSUMES )
	names.push_back( source_name(e) );
    return names;
  }
  names_t produces_names(const vertex_t& v, const BoostGraph& g) {
    names_t names;
    for ( const edge_t& e : make_iterator_range( out_edges(v,g) ) )
      if ( g[e].type == PRODUCES )
	names.push_back( target_name(e) );
    return names;
  }
  names_t inputs_names(const vertex_t& v, const BoostGraph& g) {
    names_t names;
    for ( const edge_t& e : make_iterator_range( in_edges(v,g) ) )
      names.push_back( g[e].name );
    return names;
  }
  names_t outputs_names(const vertex_t& v, const BoostGraph& g) {
    names_t names;
    for ( const edge_t& e : make_iterator_range( out_edges(v,g) ) )
      names.push_back( g[e].name );
    return names;
  }
  names_t fanin_names(const vertex_t& v, const BoostGraph& g) {
    names_t names;
    for ( const edge_t& e : make_iterator_range( in_edges(v,g) ) )
      if ( g[e].type == FANIN )
	names.push_back( source_name(e) );
    return names;
  }
  names_t fanout_names(const vertex_t& v, const BoostGraph& g) {
    names_t names;
    for ( const edge_t& e : make_iterator_range( out_edges(v,g) ) )
      if ( g[e].type == FANOUT )
	names.push_back( target_name(e) );
    return names;
  }

  string make_splitjoin_name(const subgraph_idx_t subgraph) const {
    return "splitjoin_" + std::to_string(subgraph);
  }

  // CnC_Graph needs to be a class implementing add_count, add_split, add_join, add_splitjoin_stage, add_splitjoin and add_stage  
  template<typename CnC_Graph>
  void generate_cnc_graph(CnC_Graph& g) {
    // 1. loop over all vertices and process nodes only in root graph
    //   collect various subgraph nodes in a associate container for the second pass
    std::multimap<subgraph_idx_t, vertex_t> subgraph_vertices;
    auto v_range = boost::vertices(m_graph);
    for ( const vertex_t& v : boost::make_iterator_range( v_range ) ) {
      const auto& v_props = m_graph[v];
      if ( v_props.subgraph == THE_ROOT_GRAPH ) {
	if ( v_props.type == STAGE_COL ) {
	  const string name = v_props.name;
	  const string cmd  = v_props.command;
	  const names_t consumes = consumes_names( v, m_graph );
	  const names_t produces = produces_names( v, m_graph );
	  const names_t input_ports   = inputs_names( v, m_graph );
	  const names_t output_ports = outputs_names( v, m_graph );
	  g.add_stage( name,
		       cmd,
		       consumes,
		       produces,
		       input_ports,
		       output_ports);
	}
      }
      else {
	subgraph_vertices.emplace(v_props.subgraph, v);
      }
    }

    // we cannot use BGL to check what subgraph a vertex is in
    //  we use the 'seen' vector to remember what vertices we visited in the splitjoin subgraphs
    //  if seen_p(v) == false, then v is not in any splitjoin subgraphs
    // auto seen_p = [&seen](const vertex_t& v) -> bool {
    //   return find( seen.begin(), seen.end(), v) != seen.end();
    // };

    // 2. loop over all subgraph nodes
    // We assume (for now) that is only one level of nesting for subgraphs/splitjoin
    // Note: the splitgraph creation interface (on the CnC_Graph object) uses a creation 'mode'
    //  1. announce begin splitjoin
    //  2. add split, join, count and stages in any order
    //  3. announce end of splitjoin creation

    // loop through all ordered (subgraph, vertex) entries
    for ( auto&& entry_it = subgraph_vertices.cbegin() ;
	  entry_it != subgraph_vertices.cend() ;) { // note: we're advancing manually in the body!
      subgraph_idx_t subgraph;
      vertex_t v;
      std::tie(subgraph, v) = *entry_it;
      string splitjoin_name = make_splitjoin_name(subgraph);

      g.begin_splitjoin( splitjoin_name );

      string count_command;

      // loop over all nodes in the splitjoin subgraph
      do {
	std::tie(subgraph, v) = *entry_it;
	const string& name = m_graph[v].name;
	const auto&   type = m_graph[v].type;
	// we assume the split and join nodes are well behaved:
	//  - produce/consume edges go to dependencies outside the splitjoin graph (global graph)
	//  - fanout edges go to dependencies inside the splitjoin graph
	//  - exactly one split and join node
	if ( type == STAGE_COL ) {
	  const string command = m_graph[v].command;
	  if ( name == "split" )
	    g.splitjoin_add_split( splitjoin_name, 
                                   command,
				   consumes_names( v, m_graph ),
				   produces_names( v, m_graph ),
				   fanout_names(v, m_graph) );
	  else if ( name == "join" )
	    g.splitjoin_add_join( splitjoin_name,
				  command,
				  consumes_names( v, m_graph ),
				  produces_names( v, m_graph ),
				  fanin_names(v, m_graph) );		
	  else // a regular splitjoin pipeline stage
	    g.splitjoin_add_stage( splitjoin_name,
				   name,
				   command,
				   consumes_names(v, m_graph),
				   produces_names(v, m_graph) );
	}
	else if ( type == COUNT ) {
	  count_command = m_graph[v].command;
	  g.splitjoin_add_count( splitjoin_name, count_command );
	}

	entry_it++;
	
      } while ( entry_it != subgraph_vertices.cend() &&
		entry_it->first == subgraph );
      // validate, 
      assert( ! count_command.empty() );

      g.finish_splitjoin( splitjoin_name );

    }
  }
  
  std::ostream& write_graphviz(std::ostream& out) const {
    out << "digraph pipeline {" << std::endl;
    // 1. print out all edges in 0->1 form
    auto e_range = edges(m_graph);
    std::for_each( e_range.first, e_range.second, [&out, this](const edge_t& e) {
	auto&& edge_type = m_graph[e].type;
	auto&& edge_name = m_graph[e].name;
	const auto& idx_source = get(vertex_index, m_graph, source(e, m_graph));
	const auto& idx_target = get(vertex_index, m_graph, target(e, m_graph));
	out <<  idx_source << "->" << idx_target;
	if ( !edge_name.empty() ) {
	  out << " [label=\"" << edge_name << "\"]";
	}
	out << ";";
	if ( (edge_type == FANIN) || (edge_type == FANOUT) ) {
	  out <<  idx_source << "->" << idx_target << ";";
	  out <<  idx_source << "->" << idx_target << ";";
	} // print out three edges to make clear that this is a split or join
	out << std::endl;
      });

    // 2. print out 'root graph' nodes with decoration
    std::multimap<subgraph_idx_t, vertex_t> subgraph_vertices;
    auto v_range = vertices(m_graph);
    for ( const vertex_t& v : make_iterator_range( v_range ) ) {
      const auto& v_props = m_graph[v];
      if ( v_props.subgraph == THE_ROOT_GRAPH ) {
	const auto& type  = v_props.type;
	const auto& name  = v_props.name;
	out << "    " << get(vertex_index, m_graph, v);
	if ( type == STAGE_COL )
	  out << "[label=\"" <<  name << "\",style=\"filled\",color=\".7 .3 1.0\"]";
	else if ( type == DATA_COL )
	  out << "[label=\"" <<  name << "\",shape=\"rectangle\"]";
	else if ( type == COUNT ) {
	  std::cerr << "WARNING: found \"count\" clause outside of a splitjoin." << std::endl;
	}
	out << std::endl;
      }
      else {
	subgraph_vertices.emplace(v_props.subgraph, v);
      }
    }

    // 3. Go over subgraph (splitjoin) nodes
    // We assume here that is only one level of nesting for subgraphs/splitjoin
    for ( auto&& entry_it = subgraph_vertices.cbegin() ;
	  entry_it != subgraph_vertices.cend() ;) { // note: we're advancing manually in the body!
      subgraph_idx_t subgraph;
      vertex_t v;
      std::tie(subgraph, v) = *entry_it;
      string splitjoin_name = make_splitjoin_name(subgraph);
      out << "  subgraph " << "cluster_" + splitjoin_name << " {" << std::endl;
      do {
	std::tie(subgraph, v) = *entry_it;
	const auto& v_props = m_graph[v];
	const auto& type  = v_props.type;
	const auto& name  = v_props.name;
	out << "    " << get(vertex_index, m_graph, v);
	if ( type == STAGE_COL )
	  out << "[label=\"" <<  name << "\",style=\"filled\",color=\".7 .3 1.0\"]";
	else if ( type == DATA_COL )
	  out << "[label=\"" <<  name << "\",shape=\"rectangle\"]";
	else if ( type == COUNT ) {
	  out << "[label=\"count\"]";
	}
	out << std::endl;
     	entry_it++;
	
      } while ( entry_it != subgraph_vertices.cend() &&
		entry_it->first == subgraph );
      out << "}" << std::endl;
    }

    out << "}";
    return out;		 
  }
  
};
		  
		  


#endif
