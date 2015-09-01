//********************************************************************************
// Copyright (c) 2013-2013 Intel Corporation. All rights reserved.              **
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

#ifndef PARSE_JSON_H
#define PARSE_JSON_H

#include "libjson.h"

#include <string>
#include <vector>
#include <algorithm>
#include <functional>
#include <fstream>

#include "pipeline_graph.hpp"

using std::string;
using std::cerr;
using std::endl;
using std::vector;

extern int LOG_LEVEL;
#define PARSE_LOG( expr ) if ( LOG_LEVEL > 0 ) { cerr << expr << endl; }

void parse_dependencies( const std::function<void(const JSONNode &)> & wire_fn,
			 const JSONNode & n ) {
  if( n.type() != JSON_ARRAY )
    wire_fn(n);
  else
    std::for_each( n.begin(), n.end(),
      wire_fn ); 
}

void parse_cwl_step( const JSONNode & node, PLGraph & g ) {
  auto inputs_node_it  = node.find( "inputs" );
  auto outputs_node_it = node.find( "outputs" );
  auto run_node_it     = node.find( "run" );
  auto id_node_it      = node.find( "id" );

  assert(  inputs_node_it != node.end() &&
	  outputs_node_it != node.end() &&
	      run_node_it != node.end() &&
	       id_node_it != node.end()    );
	  
  auto inputs_node  = *inputs_node_it;
  auto outputs_node = *outputs_node_it;
  auto run_node     = *run_node_it;
  auto id_node      = *id_node_it;
  
  
  PARSE_LOG( " parsing stage with id: " + id_node.as_string() );
  
  string run_string = run_node.write_formatted();
  
  auto vertex = g.stage_vertex( id_node.as_string(), run_string );

  for ( auto& in : inputs_node ) {
    //    in["id"]  // ignoring port's id for now
    auto dependency = g.dependency_vertex( in["source"].as_string() );
    g.wire_consume( dependency, vertex, in["id"].as_string() );
    PARSE_LOG( "  parsing input;  { id: "+ in["id"].as_string() + ", source: " + in["source"].as_string() + " }" );
  }
  for ( auto& out : outputs_node ) {
    //    in["id"]  // ignoring port's id for now
    auto dependency = g.dependency_vertex( out["id"].as_string() );
    g.wire_produce( vertex, dependency, out["id"].as_string() );
  }
}

void parse_stage_node( const JSONNode & stage_j, PLGraph & g ) {
  auto in_j  = stage_j.find( "in" );
  auto out_j = stage_j.find( "out" );
  auto cmd_j = stage_j.find( "command" );

  // special case: split and join nodes have special fanin and fanout dependency nodes
  auto fanout_j = (stage_j.name() == "split") ? stage_j.find( "fanout" ) : stage_j.end();
  auto fanin_j  = (stage_j.name() == "join")  ? stage_j.find( "fanin" )  : stage_j.end();

  string stage_command( cmd_j==stage_j.end() ? "" : cmd_j->as_string() );
  if (stage_command.empty())
    std::cerr << "Warning: I am parsing a node called '" << stage_j.name() <<
      "' that does not have an associated command. Proceeding with an empty command." << std::endl;
 
  auto vertex = g.stage_vertex( stage_j.name(), stage_command );
  
  auto consumes_f = [&](const JSONNode & dep_j) {
    PARSE_LOG( "  consuming '" << dep_j.as_string() << "'" );
    g.wire_consume( g.dependency_vertex( dep_j.as_string() ),
		    vertex );
  };
  auto produces_f = [&](const JSONNode & dep_j) {
    PARSE_LOG( "  producing '" << dep_j.as_string() << "'" );
    g.wire_produce( vertex ,
		    g.dependency_vertex( dep_j.as_string() ) );
  };
  auto fanout_f = [&](const JSONNode & dep_j) {
    PARSE_LOG( "  fanout '" << dep_j.as_string() << "'" );
    g.wire_fanout( vertex ,
		   g.dependency_vertex( dep_j.as_string() ) );
  };
  auto fanin_f = [&](const JSONNode & dep_j) {
    PARSE_LOG( "  fanin '" << dep_j.as_string() << "'" );
    g.wire_fanin( g.dependency_vertex( dep_j.as_string() ),
		  vertex );
  };

  if ( in_j     != stage_j.end() )
    parse_dependencies( consumes_f, *in_j     );
  if ( out_j    != stage_j.end() )
    parse_dependencies( produces_f, *out_j    );
  if ( fanin_j  != stage_j.end() )
    parse_dependencies( fanin_f,    *fanin_j  );
  if ( fanout_j != stage_j.end() )
    parse_dependencies( fanout_f,   *fanout_j );


  // check for unknown nodes/errors
  std::vector<string> acceptable_names { "command", "in", "out", "fanin", "fanout", "split", "join", "stages", "count"};
  auto acceptable_p = [&](string& name) {
    return std::find(acceptable_names.begin(), acceptable_names.end(), name) != acceptable_names.end();
  };
  for( auto& n : stage_j ) {
    string stage_name = n.name();
    if( !acceptable_p(stage_name) )
      std::cerr << "Warning: unkown entry \'" << stage_name << '\'' << std::endl;
  }
}

void parse_count_node( const JSONNode & count_j, PLGraph & g) {
  string&& command_string=count_j.as_string();
  if (command_string.empty())
    std::cerr << "Warning: I am parsing a splitjoin that has an empty associated 'count'. You want to fix this." << std::endl; 
  g.count_vertex( command_string );
}

void parse_splitjoin_node( const JSONNode & splitjoin_j, PLGraph & g ) {
  
  auto find_and_ensure_exists = [](const JSONNode& parent, const string& node_name) {
    auto it = parent.find(node_name);
    if( it == parent.end() ) {
      throw std::domain_error("splitjoin requires a node for split, join, count and stages");
    }
    else
      return *it;
  };

  auto split_j  = find_and_ensure_exists(splitjoin_j, "split"  );
  auto join_j   = find_and_ensure_exists(splitjoin_j, "join"   );
  auto count_j  = find_and_ensure_exists(splitjoin_j, "count"  );
  auto stages_j = find_and_ensure_exists(splitjoin_j, "stages" );

  g.inside_subgraph_context( [&](){
      PARSE_LOG(" parsing split");
      parse_stage_node( split_j, g );
      PARSE_LOG(" parsing stages");
      for ( auto& n: stages_j )
	parse_stage_node( n, g );
      PARSE_LOG(" parsing join");
      parse_stage_node( join_j, g );
      PARSE_LOG(" parsing count");
      parse_count_node( count_j, g );
    });  
}

void parse_cwl_workflow(const string & json_str, PLGraph& pl_graph) {
  JSONNode root_node = libjson::parse(json_str);
  for ( auto& n : root_node) {
    if ( n.name() == "class" ) {
      assert( n.as_string() == "Workflow");
    } else if ( n.name() == "steps" ) {
      for ( auto& step_node : n ) {
	parse_cwl_step( step_node, pl_graph );
      }
    }
  }
}

// main entry point, you want to call this one
void parse_json_pipeline_graph( const string & json_str, PLGraph& pl_graph ) {
  JSONNode root_node = libjson::parse(json_str);
  for ( auto& n : root_node) {
    /* if( ( n.name() != "stages" ) ) { */
    /* 	std::cerr << "expected JSON object with \"stages\", but found " << n.name() << " "  << n.as_string() << std::endl; exit( 1 ); */
    /* } */
    if ( n.name() == "stages" ) {
      PARSE_LOG("parsing stages");
      for ( auto& node : n ) {
	PARSE_LOG("parsing " << node.name());
	// nodes called splitjoin are a special case
	if ( node.name() == "splitjoin" )	  
	  parse_splitjoin_node( node, pl_graph );
	else
	  parse_stage_node( node, pl_graph );
      }
    }
  }
}

// quick and dirty input schema parser that simply returns a vector of all input id fields.
//  A more correct and elaborate version should also retrieve the 'type' information and 'description' to help buid a more sane command line user experience.
vector<string> parse_cwl_input_schema( const string & json_str ) {
  vector<string> input_arg_ids;
  JSONNode root_node = libjson::parse(json_str);
  auto inputs_it = root_node.find("inputs");
  if( inputs_it == root_node.end() ) {
    cerr << "Warning: Cannot find the 'inputs' schema in the given CWL workflow, bailing out" << endl;
    return vector<string>{};
  }
  for ( auto& n : *inputs_it ) {
    auto arg_it = n.find("id");
    if( arg_it == n.end() ) {
      cerr << "Warning: Cannot find mandatory 'id' field for an argument of the 'inputs' schema, bailing out" << endl;
      return vector<string>{};
    }
    input_arg_ids.push_back( arg_it->as_string() );
  }
  return input_arg_ids;
}

#endif
