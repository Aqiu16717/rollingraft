/**
 * @file export.h
 * @brief Symbol visibility macros
 *
 * Defines macros for controlling symbol visibility when building
 * shared libraries. Currently a placeholder for future use.
 */

#pragma once

// Placeholder for symbol export macros
// When building shared libraries, these would expand to:
// - ROLLINGRAFT_EXPORT: export symbol from library
// - ROLLINGRAFT_IMPORT: import symbol from library
// - ROLLINGRAFT_LOCAL: hide symbol from export

#ifndef ROLLINGRAFT_EXPORT
#define ROLLINGRAFT_EXPORT
#endif

#ifndef ROLLINGRAFT_IMPORT
#define ROLLINGRAFT_IMPORT
#endif

#ifndef ROLLINGRAFT_LOCAL
#define ROLLINGRAFT_LOCAL
#endif
