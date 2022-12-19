#ifdef __pgunit_h__
#error "Double #include of pgunit.h"
#endif

#define __pgunit_h__ 1

#define BUFSZ 8192

typedef struct {
	const char *name;
	void (*foo)(void);
} PBK_test_description;

extern pioDrive_i drive;
extern pioDBDrive_i dbdrive;
extern int should_be_remote;

void init_test_drives(void);
int USE_LOCAL(void);
char *random_path(void);
char *random_name(void);
void pbk_add_tests(int (*init)(void), const char *sub_name, PBK_test_description *tests);
void pio_write(pioDrive_i drive, path_t name, const char *data);
bool pio_exists(pioDrive_i drive, path_t path);
bool pio_exists_d(pioDrive_i drive, path_t path);
void copy_file(const char *from, const char *to);
void init_fake_server(const char *path);
