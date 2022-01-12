#include <errno.h>
#include <malloc.h>
#include "HashMap.h"
#include <string.h>
#include "path_utils.h"
#include "Node.h"

#include "Tree.h"

// Żeby ładniej działały nam zależności pomiędzy plikami,
// tworzymy osobny struct na drzewo
typedef struct Tree {
    Node *root;
} Tree;

Tree *tree_new() {
    Tree *t = malloc(sizeof(Tree));
    if (t == NULL)
        fatal("Memory allocation failed");
    t->root = node_new(NULL);
    return t;
}

void tree_free(Tree *t) {
    node_free(t->root);
    free(t);
}

char *tree_list(Tree *tree, const char *path) {
    if (!is_path_valid(path))
        return NULL;
    // Protokół wstępny, jeśli nie znajdziemy wierzchołka, zwracamy NULL
    if (!start_read(tree->root, path))
        return NULL;
    Node *current = get_node(tree->root, path);
    // Sekcja krytyczna
    char *result = make_map_contents_string(get_children(current));
    // Protokół końcowy
    release_held_readlocks(current, current);
    return result;
}

int tree_move(Tree *tree, const char *source, const char *target) {
    if (strcmp(source, "/") == 0)
        return EBUSY;
    // Rozważamy ten przypadek na początku, żeby nie zajmować się potem tym,
    // czym jest rodzic roota.
    if (strcmp(target, "/") == 0)
        return EEXIST;
    if (!is_path_valid(target) || !is_path_valid(source))
        return EINVAL;
    char dest_name[MAX_FOLDER_NAME_LENGTH + 1];
    char source_name[MAX_FOLDER_NAME_LENGTH + 1];
    char *target_parent = make_path_to_parent(target, dest_name);
    char *source_parent = make_path_to_parent(source, source_name);
    // Protokół wstępny
    if (!start_write(tree->root, source_parent, target_parent)) {
        free(target_parent);
        free(source_parent);
        return ENOENT;
    }
    Node *source_node = get_node(tree->root, source_parent),
            *target_node = get_node(tree->root, target_parent);
    Node *to_move = hmap_get(get_children(source_node), source_name);
    // Jeśli nie istnieje wierzchołek, który chcemy przenieść
    if (to_move == NULL) {
        end_write(source_node, target_node);
        free(target_parent);
        free(source_parent);
        return ENOENT;
    }
    // Jeśli przeniesienie by nic nie zrobiło
    if (strcmp(source, target) == 0) {
        end_write(source_node, target_node);
        free(target_parent);
        free(source_parent);
        return 0;
    }
    // Jeśli chcemy przenieść folder do swojego własnego podfolderu
    if (strncmp(source, target, strlen(source)) == 0) {
        end_write(source_node, target_node);
        free(target_parent);
        free(source_parent);
        return -1;
    }
    // Jeśli istnieje już docelowy wierzchołek
    if (get_node(tree->root, target) != NULL) {
        end_write(source_node, target_node);
        free(target_parent);
        free(source_parent);
        return EEXIST;
    }
    // Sekcja krytyczna
    set_father(to_move, target_node);
    hmap_remove(get_children(source_node), source_name);
    hmap_insert(get_children(target_node), dest_name, to_move);
    // Protokół końcowy
    end_write(source_node, target_node);
    free(target_parent);
    free(source_parent);
    return 0;
}

int tree_create(Tree *tree, const char *path) {
    if (!is_path_valid(path))
        return EINVAL;
    Node *node;
    char name[MAX_FOLDER_NAME_LENGTH + 1];
    char *parent = make_path_to_parent(path, name);
    // Jeśli chcemy stworzyć folder /
    if (parent == NULL)
        return EEXIST;
    // Protokół wstępny
    if (!start_write(tree->root, parent, parent)) {
        free(parent);
        return ENOENT;
    }
    node = get_node(tree->root, parent);
    // Jeśli istnieje już wierzchołek, który chcemy stworzyć
    if (hmap_get(get_children(node), name) != NULL) {
        end_write(node, node);
        free(parent);
        return EEXIST;
    }
    Node *new = node_new(node);
    // Sekcja krytyczna
    hmap_insert(get_children(node), name, new);
    // Protokół końcowy
    end_write(node, node);
    free(parent);
    return 0;
}

int tree_remove(Tree *tree, const char *path) {
    if (!is_path_valid(path))
        return EINVAL;
    if (strcmp(path, "/") == 0)
        return EBUSY;
    Node *node;
    char name[MAX_FOLDER_NAME_LENGTH + 1];
    char *parent = make_path_to_parent(path, name);
    // Protokół wstępny
    if (!start_write(tree->root, parent, parent)) {
        free(parent);
        return ENOENT;
    }
    node = get_node(tree->root, parent);
    Node *old = hmap_get(get_children(node), name);
    // Jeśli nie istnieje wierzchołek, który chcemy usunąć
    if (old == NULL) {
        end_write(node, node);
        free(parent);
        return ENOENT;
    }
    // Jeśli wierzchołek ma dzieci
    if (hmap_size(get_children(old)) != 0) {
        end_write(node, node);
        free(parent);
        return ENOTEMPTY;
    }
    // Sekcja krytyczna — tutaj mamy writelock na node, więc tym bardziej
    // mamy wyłączny dostęp do old
    node_free(old);
    hmap_remove(get_children(node), name);
    end_write(node, node);
    free(parent);
    return 0;
}