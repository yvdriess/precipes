//********************************************************************************
// Copyright (c) 2013-2015 Yves Vandriessche and Frank Schlimbach.              **
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


// Parser for defining a pipeline in JSON.
//
// A stage of a pipeline needs to define 3 things: the command-line to be executed, input-dependences and output-dependences.
// Each pipeline "instance" is prescribed with an input, its name can be referred to as {} in the definitions of the command lines.
// The input- and output dependences are modeled only, no data is actually transferred. E.g. you might think of files that get
// passed from one stage to the next. It's the responsibility of the individual stage that the output (file) is actually available to the 
// consuming steps before it finishes. The runtime will assume that the defined dependences are available once the stage has finished.
//
// JSON syntax:
// Each stage has three fields: "command": the command line to execute, {} will be replaced with the input.
//                              "in"  : names of input collections/dependences (single string or array of strings)
//                              "out" : name of output collections/dependences (single string or array of strings)
//
// Example
// {
//   "stages" : {
//     "stage1" : {
//         // with input "foo" expands to "compute1() > foo.out1"
//         "command" : "compute1() > {}.out1",
//         "out" : "out1"
//     },
//     "stage2" : {
//         // with input "foo" expands to "compute2(foo.out1) > {}.out2"
//         "command" : "compute2({}.out1) > {}.out2",
//         "in": "out1",
//         "out" : "out2"
//     },
//     "stage3" : {
//         // with input "foo" expands to "compute3(foo.out1, foo.out2) | tee foo.out3a > foo.out3b"
//         "command" : "compute3({}.out1, {}.out2) | tee {}.out3a > {}.out3b",
//         "in": ["out1", "out2"],
//         "out" : ["out3a", "out3b"]
//     }
//   }
// }

#ifndef _PARSE_JSON_INCLUDED_
#define _PARSE_JSON_INCLUDED_

#include "libjson.h"
#include <fstream>
#include <string>

struct json_init_pipeline
{
	template< typename PL >
	json_init_pipeline( PL & pl, const std::string & json_str ) {
		JSONNode n = libjson::parse(json_str);
		init( pl, n );
	}

	template< typename PL >
	static void init( PL & pl, const JSONNode & n )
	{
		std::string pname;
		auto i = n.begin();
		while( i != n.end() ) {
			// recursively call ourselves to dig deeper into the tree
			if( ( i->name() != "stages" && i->name() != "data-collections" ) ) {
				std::cerr << "expected JSON object with \"stages\" and/or \"data-collections\", but found " << i->name() << " "  << i->as_string() << std::endl; exit( 1 );
			}
			if( i->name() == "stages" ) {
				for( JSONNode::const_iterator j = i->begin(); j != i->end(); ++j ) {
					std::string cmd;
					data_deps_type ins, outs;
					deps_names_type in_names, out_names;
					ParseStage( pl, *j, cmd, ins, outs, in_names, out_names );
					pl.add_stage( Stage( cmd, ins, outs, in_names, out_names ), j->name() );
				}
			} else if( i->name() == "data-collections" ) { 
				if( i->type() != JSON_ARRAY ) {
					pl.add_data_coll( i->as_string() );
				} else {
					for( auto d = 0; d < i->size(); ++d ) {
						std::cout << "kk " << i->size() << " " << (*i)[d].as_string() << std::endl;
						pl.add_data_coll( (*i)[d].as_string() );
					}
				}
			}
			//increment the iterator
			++i;
		}
	}
private:
	
	template< typename PL >
	static void ParseDeps( PL & pl, const JSONNode & n, data_deps_type & deps, deps_names_type & names)
	{
	  if( n.type() != JSON_ARRAY ) {
	    const std::string dep_name( n.as_string() );
	    deps.push_back( pl.get_data_coll( dep_name ) );
	    names.push_back( dep_name );
	  }
	  for( int s = 0; s < n.size(); ++s ) {
	    const std::string dep_name( n[s].as_string() );
	      deps.push_back( pl.get_data_coll( dep_name ) );
	      names.push_back( n[s].as_string() );
	    }
	}
	
	template< typename PL >
	static void ParseStage( PL & pl, const JSONNode & n, std::string & cmd,
				data_deps_type & ins, data_deps_type & outs,
				deps_names_type & in_names, deps_names_type & out_names )
	{    
		for( JSONNode::const_iterator i = n.begin(); i != n.end(); ++i ) {
			if( i->name() == "command" ) cmd = i->as_string();
			else if( i->name() == "in" )  ParseDeps( pl, *i, ins, in_names );
			else if( i->name() == "out" ) ParseDeps( pl, *i, outs, out_names );
			else std::cerr << "ignoring unkown entry " << i->name() << std::endl;
		}
	}

};

#endif //_PARSE_JSON_INCLUDED_
