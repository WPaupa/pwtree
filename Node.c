#include "Node.h"
#include <errno.h>
#include <malloc.h>
#include <string.h>

// Konwencja używana w synchronizacji:
// z funkcji czytających z hashmapy children (jak find i iteratora) i pola
// father można korzystać, jeśli się jest czytelnikiem lub pisarzem danego Node,
// z funkcji modyfukujących hashmapę (jak insert i remove) oraz pole father
// tylko jeśli się jest pisarzem danego node, a z pozostałych zmiennych
// składowych Node można korzystać tylko, gdy się ma mutexa.
typedef struct Node {
    HashMap *children;
    // Zmienne warunkowe do czekania na dostęp do czytelni
    pthread_cond_t readlock, writelock;
    // Zmienne warunkowe do czekania na wyzerowanie stanu semafora
    // (jeśli semafor jest podniesiony, to czekamy aż proces, dla którego
    // został podniesiony, go opuści, a dopiero potem wchodzimy do protokołu)
    pthread_cond_t rprio, wprio;
    // Zmienne przechowujące stan semafora
    int rstate, wstate;
    // Odpowiednio liczba czytelników i pisarzy
    // odpowiednio czekających i działających
    int rwait, wwait, rrun, wrun;
    struct Node *father;
    // Wysokość — aktualizowana przy zdobywaniu locków, czytana przy oddawaniu.
    int height;
    pthread_mutex_t mutex;
} Node;

Node *get_father(Node *node) {
    if (node == NULL)
        return NULL;
    return node->father;
}

HashMap *get_children(Node *node) {
    return node->children;
}

void set_father(Node *node, Node *father) {
    node->father = father;
}

int get_height(Node *node) {
    if (node == NULL)
        return 0;
    ptry(pthread_mutex_lock(&node->mutex));
    int h = node->height;
    ptry(pthread_mutex_unlock(&node->mutex));
    return h;
}

void set_height(Node *node, int h) {
    if (node == NULL)
        return;
    ptry(pthread_mutex_lock(&node->mutex));
    node->height = h;
    ptry(pthread_mutex_unlock(&node->mutex));
}

Node *node_new(Node *father) {
    Node *n = malloc(sizeof(Node));
    if (n == NULL)
        fatal("Memory allocation failed");
    n->children = hmap_new();
    n->rwait = n->wwait = n->rrun = n->wrun = 0;
    n->rstate = n->wstate = 0;
    n->father = father;
    ptry(pthread_cond_init(&n->readlock, 0));
    ptry(pthread_cond_init(&n->writelock, 0));
    ptry(pthread_cond_init(&n->rprio, 0));
    ptry(pthread_cond_init(&n->wprio, 0));
    ptry(pthread_mutex_init(&n->mutex, 0));
    set_height(n, get_height(father) + 1);
    return n;
}

void node_free(Node *node) {
    const char *child_name;
    Node *child;
    ptry(pthread_mutex_lock(&node->mutex));
    // Rekurencyjnie zwalniamy wszystkie dzieci, pod mutexem dla pewności
    for (HashMapIterator it = hmap_iterator(node->children);
            hmap_next(node->children, &it,
                      &child_name, (void **) &child);
            node_free(child));
    hmap_free(node->children);
    ptry(pthread_mutex_unlock(&node->mutex));
    ptry(pthread_cond_destroy(&node->writelock));
    ptry(pthread_cond_destroy(&node->readlock));
    ptry(pthread_cond_destroy(&node->rprio));
    ptry(pthread_cond_destroy(&node->wprio));
    ptry(pthread_mutex_destroy(&node->mutex));
    free(node);
}

Node *get_node(Node *root, const char *path) {
    const char *subpath = path;
    char component[MAX_FOLDER_NAME_LENGTH + 1];
    Node *current = root;
    while ((subpath = split_path(subpath, component))) {
        if ((current = hmap_get(current->children, component)) == NULL)
            return NULL;
    }
    return current;
}

void get_readlock(Node *current) {
    if (current == NULL)
        return;
    ptry(pthread_mutex_lock(&current->mutex));
    // Jeśli komuś jest przekazana sekcja krytyczna, czekamy, aż ją przejmie
    while (current->rstate > 0)
        ptry(pthread_cond_wait(&current->rprio, &current->mutex));
    // Jeśli pisarz jest w czytelni lub niedługo będzie, to czekamy
    if (current->wrun + current->wwait + current->wstate > 0) {
        current->rwait++;
        // Czekamy na podniesienie semafora
        while (current->rstate == 0)
            ptry(pthread_cond_wait(&current->readlock, &current->mutex));
        current->rstate--;
        current->rwait--;
        // Jeśli semafor jest już pusty, to budzimy procesy czekające na niego.
        if (current->rstate == 0)
            ptry(pthread_cond_broadcast(&current->rprio));
    }
    current->rrun++;
    ptry(pthread_mutex_unlock(&current->mutex));
}

void release_readlock(Node *current) {
    if (current == NULL)
        return;
    ptry(pthread_mutex_lock(&current->mutex));
    current->rrun--;
    // Jeśli jest pusta czytelnia (i nikt nie ma do niej za chwilę wejść),
    // to budzimy kogoś
    if (current->rrun == 0 && current->wrun == 0 &&
        current->rstate == 0 && current->wstate == 0) {
        // Jeśli czekają pisarze, to pisarza
        if (current->wwait > 0) {
            current->wstate = 1;
            ptry(pthread_cond_signal(&current->writelock));
        } else {
            // A jeśli nie, to czytelników
            current->rstate = current->rwait;
            ptry(pthread_cond_broadcast(&current->readlock));
        }
    }
    ptry(pthread_mutex_unlock(&current->mutex));
}

void release_held_readlocks(Node *node1, Node *node2) {
    while (node1 != NULL || node2 != NULL) {
        // Oddajemy najpierw na głębszym wierzchołku, bo on może
        // być potomkiem płytszego.
        if (get_height(node1) > get_height(node2)) {
            release_readlock(node1);
            node1 = get_father(node1);
        } else if (get_height(node1) < get_height(node2)) {
            release_readlock(node2);
            node2 = get_father(node2);
        } else {
            release_readlock(node1);
            // Jeśli wierzchołki są równe, oddajemy tylko jeden lock
            if (node1 != node2) {
                release_readlock(node2);
            }
            node1 = get_father(node1);
            node2 = get_father(node2);
        }
    }
}

bool start_read(Node *root, const char *path) {
    char component[MAX_FOLDER_NAME_LENGTH + 1];
    Node *node = root;
    int h = 1;
    const char *subpath = path;
    while ((subpath = split_path(subpath, component))) {
        // Dostajemy readlocka, począwszy od roota
        get_readlock(node);
        Node *new = hmap_get(node->children, component);
        if (new == NULL) {
            // Jeśli nie ma takiego wierzchołka, musimy oddać readlocki
            // i o tym powiadomić wołającego
            release_held_readlocks(node, node);
            return false;
        }
        // Definicja height zmusza nas do aktualizowania jej przy
        // zdobywaniu readlocków.
        set_height(new, ++h);
        node = new;
    }
    // Zostało jeszcze nam dostać readlocka na ostatnim wierzchołku.
    get_readlock(node);
    return true;
}

bool get_writelock(Node *current) {
    if (current == NULL)
        return false;
    ptry(pthread_mutex_lock(&current->mutex));
    // Jeśli semafor jest podniesiony, czekamy, aż ktoś przez niego przejdzie
    while (current->wstate > 0)
        ptry(pthread_cond_wait(&current->wprio, &current->mutex));
    // Jeśli ktoś jest w czytelni (lub już ma wejść, pisarz nie wejdzie teraz,
    // bo czekaliśmy na wstate = 0), to czekamy
    if (current->rrun + current->wrun + current->rstate > 0) {
        current->wwait++;
        while (current->wstate == 0)
            ptry(pthread_cond_wait(&current->writelock, &current->mutex));
        current->wstate--;
        current->wwait--;
        // Zmniejszamy stan semafora i jeśli go wyzerowaliśmy, budzimy
        // czekających na to pisarzy.
        if (current->wstate == 0)
            ptry(pthread_cond_broadcast(&current->wprio));
    }
    // Ze względu na semantykę to się musi wykonać, zanim do mutexa
    // wejdą procesy z broadcasta na wprio.
    current->wrun++;
    ptry(pthread_mutex_unlock(&current->mutex));
    return true;
}

void release_writelock(Node *current) {
    ptry(pthread_mutex_lock(&current->mutex));
    current->wrun--;
    // Jeśli czytelnia jest pusta (wrun = 0 jest zagwarantowane, ale
    // sprawdzamy dla bezpieczeństwa), to możemy kogoś wpuścić
    if (current->rrun == 0 && current->wrun == 0 &&
        current->rstate == 0 && current->wstate == 0) {
        // Jeśli czekają czytelnicy, to wszystkich czytelników
        if (current->rwait > 0) {
            current->rstate = current->rwait;
            ptry(pthread_cond_broadcast(&current->readlock));
        } else if (current->wwait > 0) {
            // A jeśli nie, to pisarza (o ile jakiś czeka)
            current->wstate = 1;
            ptry(pthread_cond_signal(&current->writelock));
        }
    }
    ptry(pthread_mutex_unlock(&current->mutex));
}

bool start_write(Node *root, const char *path1, const char *path2) {
    int cmp = strcmp(path1, path2);
    // Upewniamy się, że path2 nie jest prefixem niewłaściwym path1
    // i jednocześnie zabezpieczamy się przed deadlockiem.
    if (cmp > 0) {
        const char *tmp = path1;
        path1 = path2;
        path2 = tmp;
    }
    char component1[MAX_FOLDER_NAME_LENGTH + 1], component2[
            MAX_FOLDER_NAME_LENGTH + 1];
    Node *node1 = root, *node2 = root;
    int h1 = 1, h2 = 1;
    const char *subpath1 = path1, *subpath2 = path2;
    while ((subpath1 = split_path(subpath1, component1))) {
        // Zdobywamy readlocki na pierwszej ścieżce
        get_readlock(node1);
        Node *new1 = hmap_get(node1->children, component1);
        if (new1 == NULL) {
            // Tak samo, jak przy czytaniu: oddajemy, jeśli nie znaleźliśmy
            release_held_readlocks(node1, node1);
            return false;
        }
        set_height(new1, ++h1);
        // Dopóki ścieżki się pokrywają, z drugim wierzchołkiem też schodzimy,
        // ale nie zdobywamy żadnych locków
        if (node1 == node2) {
            subpath2 = split_path(subpath2, component2);
            if ((node2 = hmap_get(node2->children, component2)) == NULL) {
                release_held_readlocks(node1, node1);
                return false;
            }
            set_height(node2, ++h2);
        }
        node1 = new1;
    }
    // Zdobywamy writelock na ostatnim wierzchołku pierwszej ścieżki
    get_writelock(node1);
    // W tym miejscu node2 jest ostatnim wspólnym wierzchołkiem ścieżek.
    while ((subpath2 = split_path(subpath2, component2))) {
        if (node1 == node2) {
            // W tym przypadku mamy już writelocka na node2, więc nie możemy
            // zdobyć tam readlocka konwencjonalnie, więc po prostu
            // zwiększamy liczbę czytelników. Prowadzi to do złamania warunku
            // czytelników i pisarzy, ale dzieje się to w kontrolowany sposób,
            // tzn to jest jedyny sposób na złamanie tego warunku i protokoły
            // końcowe sobie z tym radzą.
            ptry(pthread_mutex_lock(&node2->mutex));
            node2->rrun++;
            ptry(pthread_mutex_unlock(&node2->mutex));
        } else
            get_readlock(node2);
        Node *new = hmap_get(node2->children, component2);
        if (new == NULL) {
            release_writelock(node1);
            release_held_readlocks(get_father(node1), node2);
            return false;
        }
        set_height(new, ++h2);
        node2 = new;
    }
    // Jeśli node1 =/= node2, to musimy zdobyć drugi writelock
    if (cmp != 0)
        get_writelock(node2);
    return true;
}

void end_write(Node *node1, Node *node2) {
    // Oddajemy dwa writelocki (chyba że node1 = node2, to wtedy jeden)
    // i readlocki od ich ojców do korzenia
    release_writelock(node1);
    if (node1 != node2)
        release_writelock(node2);
    release_held_readlocks(get_father(node1), get_father(node2));
}