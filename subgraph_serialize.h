/**
 *  Serialization of boost::subgraph
 *
 *  Created by Rakuto Furutani <xri://rakuto/> (rakuto+nospam@gmail.com)
 */
#ifndef SUBGRAPH_SELIALIZE_H
#define SUBGRAPH_SELIALIZE_H

#include <boost/graph/adj_list_serialize.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/subgraph.hpp>

namespace boost {
namespace serialization {

template<class Archive, class Graph>
inline void save(Archive& ar, 				 
				 const subgraph<Graph> graph, 
				 const unsigned int /* file_version*/) 
{
	typedef typename subgraph<Graph>::vertex_descriptor Vertex;
	typedef typename subgraph<Graph>::const_children_iterator ChildrenIter;

	// serialize global verticies and edges
	size_t num_children = graph.num_children();
	bool root = graph.is_root();
	ar << BOOST_SERIALIZATION_NVP(root);
	ar << BOOST_SERIALIZATION_NVP(num_children);
	ar << BOOST_SERIALIZATION_NVP(graph.m_graph);
	ChildrenIter ci, ci_end;
	tie(ci, ci_end) = graph.children();
	for(; ci != ci_end; ++ci) {
		ar << *ci;
	}
}
	
template<class Archive, class Graph>
inline void load(
				 Archive& ar,
				 subgraph<Graph>& graph,
				 const unsigned int /* file_version */)
{
	typedef typename subgraph<Graph>::vertex_descriptor Vertex;
	typedef typename Graph::vertex_iterator VertexIter;
	Graph g;
	bool root;
	size_t num_children;
	ar >> BOOST_SERIALIZATION_NVP(root);
	ar >> BOOST_SERIALIZATION_NVP(num_children);
	ar >> BOOST_SERIALIZATION_NVP(g);

	if(root) {
		graph.m_graph = g;
	} else {
		VertexIter vi, vi_end;
		tie(vi, vi_end) = vertices(g);
		graph.create_subgraph(vi, vi_end);
	}
	for(size_t i = 0; i < num_children; ++i) {
		ar >> BOOST_SERIALIZATION_NVP(graph);
	}
}

template<class Archive, class Graph>
inline void serialize(
					  Archive& ar,
					  boost::subgraph<Graph>& graph,
					  const unsigned int file_version)	
{
	boost::serialization::split_free(ar, graph, file_version);
}
	
} // namespace serialization
} // namespace boost

#endif
