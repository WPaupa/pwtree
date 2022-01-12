#pragma once

#include "err.h"
#include <pthread.h>
// dla stdbool.h
#include "path_utils.h"

// Makro do wykonywania funkcji z biblioteki pthreads z jednoczesnym
// sprawdzeniem kodu błędu. W przypadku ustawienia flagi NDEBUG na fałsz
// jednocześnie przy każdym wywołaniu pthreadowej funkcji informuje o tym
// użytkownika.
#define NDEBUG
#ifdef NDEBUG
#define ptry(x) if ((errno = x) != 0) syserr("Error in pthreads function")
#else
#define ptry(x) do { printf("%d ",__LINE__); puts(#x); if ((errno = x) != 0) \
    syserr("Error in pthreads function"); } while (0)
#endif

// Typ reprezentujący wierzchołek
typedef struct Node Node;

// Ustawia pierwszemu wierzchołkowi atrybut rodzica na drugi wierzchołek.
void set_father(Node *node, Node *father);

// Zwraca hashmapę reprezentującą dzieci danego wierzchołka.
HashMap *get_children(Node *node);

// Stworzenie nowego wierzchołka o podanym ojcu
Node *node_new(Node *);

// Zwolnienie pamięci związanej z wierzchołkiem i wszystkimi jego potomkami
void node_free(Node *);

// Zwraca wierzchołek z podanego drzewa o podanym adresie. Uwaga: wymaga,
// żeby proces wołający był co najmniej czytelnikiem w każdym wierzchołku
// na ścieżce z korzenia do wynikowego wierzchołka.
Node *get_node(Node *root, const char *);

// Oddaje status czytelnika na wszystkich wierzchołkach na ścieżce od dwóch
// podanych wierzchołków do korzenia (jeśli się pokrywają, to oddaje tylko
// raz, w szczególności release_held_readlocks(node, node) oddaje readlock
// na wszystkich wierzchołkach od node do korzenia dokładnie raz).
void release_held_readlocks(Node *, Node *);

// Zaczyna czytanie w wierzchołku o podanej ścieżce, tj dostaje status
// czytelnika na wszystkich wierzchołkach od korzenia do tego o podanej ścieżce.
// Jeśli taki wierzchołek nie istnieje, puszcza posiadane locki i zwraca fałsz.
bool start_read(Node *root, const char *);

// Zaczyna pisanie w wierzchołkach o podanych ścieżkach, tj dostaje status
// czytelnika na wszystkich wierzchołkach na ścieżkach od korzenia do obydwu
// z nich (oprócz nich samych) oraz status pisarza w nich samych.
bool start_write(Node *root, const char *, const char *);

// Kończy pisanie w podanych wierzchołkach, tj oddaje w nich status pisarza
// i wszystkie statusy czytelnika w wierzchołkach na ścieżkach od ich ojców
// do korzenia.
void end_write(Node *, Node *);