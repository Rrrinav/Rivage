// --- GLOBAL SETUP & COLLEGE GUIDELINES ---
#set page(
  paper: "a4",
  margin: (left: 38mm, right: 25mm, top: 25mm, bottom: 25mm),
  
  // Custom double-line border perfectly framed around the text area
  // Border is exactly 10mm away from the text on all sides
  background: place(top + left, dx: 28mm, dy: 15mm)[
    // Outer thick border (3pt)
    #rect(width: 100% - 43mm, height: 100% - 30mm, stroke: 3pt)[
      // Inner thin border (0.5pt)
      #place(center + horizon)[
        #rect(width: 100% - 0.2pt, height: 100% - 0.2pt, stroke: 0.5pt)
      ]
    ]
  ]
)

#set text(
  font: "Times New Roman",
  size: 12pt,
)

// 1.5 line spacing equivalent (12pt font + 9pt leading = 21pt baseline distance)
#set par(
  leading: 0.75em,
  justify: true,
)

#set heading(numbering: "1.")

// --- TITLE PAGE ---
#align(center)[
  #v(2cm)
  #text(size: 12pt, weight: "bold")[Rivage: A Polyglot Distributed Compute Engine for Numerical and Batch Processing] \
  #v(1.5cm)
  #text(size: 12pt, weight: "bold")[Final Year Engineering Project Report] \
  #v(1cm)
  #text(size: 12pt, style: "italic")[Draft Version] \
  #v(2cm)
]

#pagebreak()

// --- TABLE OF CONTENTS ---
#outline(title: "Table of Contents", depth: 2)

#pagebreak()

// --- MAIN CONTENT ---

// Define a custom footer to precisely center the page numbers 
// in the gap between the text block and the bottom border
#set page(
  footer: context align(center)[
    #move(dy: -6mm)[
      #counter(page).display("1")
    ]
  ]
)
#counter(page).update(1)

= Introduction

Distributed computing has become the backbone of modern data processing. As datasets grow beyond the capacity of single-node architectures, frameworks utilizing the MapReduce programming model have emerged as the industry standard. By dividing workloads into independent 'Map' tasks for data transformation and 'Reduce' tasks for aggregation, MapReduce enables highly scalable and fault-tolerant parallel processing across clusters of commodity hardware.

Despite the success of established frameworks like Apache Hadoop, they often introduce significant operational overhead. These enterprise-grade systems are monolithic and heavily coupled to specific ecosystems, primarily the Java Virtual Machine (JVM). This rigid architecture leads to steep learning curves, high memory footprints, and difficulty integrating lightweight scripts or alternative programming languages without relying on inefficient wrappers. For many general-purpose distributed computing tasks, deploying such a heavy architecture is unnecessary and cumbersome.

To solve these challenges, this project introduces "Rivage", a lightweight, high-throughput distributed MapReduce orchestration engine. Rivage is built from the ground up using Go (Golang), leveraging the language's native concurrency model and high-performance networking capabilities. Instead of relying on traditional REST APIs, Rivage utilizes bi-directional gRPC streaming to maintain persistent, low-latency communication channels between the central coordinator and distributed worker nodes.

The defining feature of Rivage is its completely polyglot execution environment. By treating worker nodes as agnostic execution vessels, the system pipes standard JSON data directly into the standard input (STDIN) of any executable script or binary. This design allows developers to write their Map and Reduce logic in Python, Go, Node.js, or any other preferred language. Rivage provides the scalability of a distributed compute engine while maintaining the simplicity and flexibility of localized script execution.

== Objectives
The primary objectives of this project are as follows:
- To design and implement a lightweight, distributed MapReduce orchestration engine focusing on high concurrency and minimal operational overhead.
- To build a highly responsive communication layer using bi-directional gRPC streams for real-time task dispatching and state synchronization.
- To architect a language-agnostic worker node that utilizes standard I/O piping, allowing tasks to be executed in any programming language without framework-specific SDKs.
- To demonstrate the engine's capability by successfully distributing and executing general-purpose batch processing workloads.

#pagebreak()
= Literature Review
[To be written: Review of existing distributed systems like Hadoop, Apache Spark, and standard MapReduce papers. Will discuss their advantages and the operational complexities/lock-ins they face.]

#pagebreak()
= Design, Setup and Methodology
[To be written: Will contain the architectural diagrams, the Go gRPC communication flow, and the implementation details of the polyglot worker piping JSON via `os/exec`.]

#pagebreak()
= Results and Discussion
[To be written: Performance benchmarks, evaluation of MapReduce jobs (e.g., word count, batch processing) running on distributed nodes. Analysis of system throughput and resource utilization.]

#pagebreak()
= Conclusions and Scope for Future Work
[To be written: Summary of findings regarding the efficiency of the polyglot approach. Future scope will include implementing fault tolerance, advanced load-aware scheduling, and external disk-based shuffling.]

// --- PAGE BREAK FOR BIO ---
#pagebreak()
#heading(numbering: none)[Brief Bio Data of the Candidate]
[To be written: One page strictly containing the candidate's academic background, skills, and project contributions.]

// --- PAGE BREAK FOR PUBLICATIONS ---
#pagebreak()
#heading(numbering: none)[Research Publications]
[To be written: List of any papers or journals published derived from this project, if applicable.]

// --- PAGE BREAK FOR REFERENCES ---
#pagebreak()
#heading(numbering: none)[References]
[1] J. Dean and S. Ghemawat, "MapReduce: Simplified Data Processing on Large Clusters," _Communications of the ACM_, vol. 51, no. 1, pp. 107-113, 2008. \
\
[2] gRPC Authors, "gRPC: A high performance, open source universal RPC framework," [Online]. Available: https://grpc.io/

// --- PAGE BREAK FOR APPENDICES ---
#pagebreak()
#set heading(numbering: "A.")
#counter(heading).update(0)

= Appendices
[To be written: Tables, extended code snippets (e.g., Protobuf definitions, worker core logic), and supplementary data not included in the main report.]
