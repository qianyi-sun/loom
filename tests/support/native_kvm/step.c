/* Trusted offline Dockerfile RUN payload; never executed on the host. */
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

int main(void) {
    if (access("/proc/gvisor/kernel_is_gvisor", F_OK) != 0) return 11;
    int fd = open("/executed", O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return 12;
    const char *marker = "native-buildkit-kvm-executed\n";
    if (write(fd, marker, strlen(marker)) != (ssize_t)strlen(marker)) return 13;
    return close(fd) == 0 ? 0 : 14;
}
